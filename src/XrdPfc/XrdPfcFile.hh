#ifndef __XRDPFC_FILE_HH__
#define __XRDPFC_FILE_HH__
//----------------------------------------------------------------------------------
// Copyright (c) 2014 by Board of Trustees of the Leland Stanford, Jr., University
// Author: Alja Mrak-Tadel, Matevz Tadel
//----------------------------------------------------------------------------------
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//----------------------------------------------------------------------------------

#include "XrdPfcTypes.hh"
#include "XrdPfcInfo.hh"
#include "XrdPfcStats.hh"

#include "XrdOuc/XrdOucCache.hh"
#include "XrdOuc/XrdOucIOVec.hh"

#include <functional>
#include <list>
#include <map>
#include <set>
#include <string>

class XrdJob;
struct XrdOucIOVec;

namespace XrdPfc
{
class File;
class Block;
class BlockRun;
class BlockRunResponseHandler;
class BlockSequenceResponseHandler;
class DirectResponseHandler;
class IO;

struct ReadVBlockListRAM;
struct ReadVChunkListRAM;
struct ReadVBlockListDisk;
struct ReadVChunkListDisk;

struct ReadReqRH : public XrdOucCacheIOCB
{
   int               m_expected_size = 0;
   int               m_n_chunks = 0; // Only set for ReadV().
   unsigned short    m_seq_id;
   XrdOucCacheIOCB  *m_iocb; // External callback passed into IO::Read().

   ReadReqRH(unsigned short sid, XrdOucCacheIOCB *iocb) :
      m_seq_id(sid), m_iocb(iocb)
   {}
};

// -------------------------------------------------------------

struct ReadRequest
{
   IO         *m_io;
   ReadReqRH  *m_rh; // Internal callback created in IO::Read().

   long long   m_bytes_read = 0;
   int         m_error_cond = 0; // to be set to -errno
   int         m_error_count = 0;
   Stats       m_stats;

   int         m_n_chunk_reqs = 0;
   bool        m_sync_done    = false;
   bool        m_direct_done  = true;

   ReadRequest(IO *io, ReadReqRH *rh) :
      m_io(io), m_rh(rh)
   {}

   void update_error_cond(int ec) { ++m_error_count; if (m_error_cond == 0 ) m_error_cond = ec; }

   bool is_complete()  const { return m_n_chunk_reqs == 0 && m_sync_done && m_direct_done; }
   int  return_value() const { return m_error_cond ? m_error_cond : m_bytes_read; }
};

// -------------------------------------------------------------

struct ChunkRequest
{
   ReadRequest *m_read_req;
   char        *m_buf;      // Where to place the data chunk.
   long long    m_off;      // Offset *within* the corresponding block.
   int          m_size;     // Size of the data chunk.

   ChunkRequest(ReadRequest *rreq, char *buf, long long off, int size) :
      m_read_req(rreq), m_buf(buf), m_off(off), m_size(size)
   {}
};

using vChunkRequest_t = std::vector<ChunkRequest>;
using vChunkRequest_i = std::vector<ChunkRequest>::iterator;

// ================================================================

//------------------------------------------------------------------------------
//! One cache block: the unit of cache *state* -- one bit in the cinfo block
//! vector, one entry in File::m_block_map.
//!
//! A Block does not own its memory. It always belongs to a BlockRun, which owns
//! the RAM allocation shared by all blocks of the run, and which is the unit of
//! *transfer*: one remote request and one disk write per run. Everything about a
//! block that describes the request it arrived on therefore lives on the run.
//------------------------------------------------------------------------------

class Block
{
public:
   BlockRun           *m_run;           // run owning the RAM buffer; never null
   char               *m_buff;          // points into m_run->m_buff
   long long           m_offset;
   int                 m_size;
   int                 m_refcnt;
   int                 m_errno;         // stores negative errno
   bool                m_downloaded;
   bool                m_prefetch;      // per block: a demand run can carry
                                        // gap-filler blocks nobody asked for

   vChunkRequest_t     m_chunk_reqs;

   Block(BlockRun *run, char *buf, long long off, int size, bool prefetch) :
      m_run(run), m_buff(buf), m_offset(off), m_size(size),
      m_refcnt(0), m_errno(0), m_downloaded(false), m_prefetch(prefetch)
   {}

   char*     get_buff()     const { return m_buff;   }
   int       get_size()     const { return m_size;   }
   long long get_offset()   const { return m_offset; }

   BlockRun* get_run()      const { return m_run;    }

   // These delegate to the run -- see BlockRun for the definitions.
   File* get_file()   const;
   IO*   get_io()     const;
   void* get_req_id() const;
   bool  req_cksum_net() const;

   bool is_finished() const { return m_downloaded || m_errno != 0; }
   bool is_ok()       const { return m_downloaded; }
   bool is_failed()   const { return m_errno != 0; }

   void set_downloaded()    { m_downloaded = true; }
   void set_error(int err)  { m_errno      = err;  }
   int  get_error() const   { return m_errno;      }

   void reset_error()       { m_errno      = 0;    }
};

using BlockList_t = std::list<Block*>;
using BlockList_i = std::list<Block*>::iterator;

// ================================================================

//------------------------------------------------------------------------------
//! A run of index-consecutive blocks sharing one RAM allocation, one remote
//! request and one disk write. This is the unit of transfer.
//!
//! When the cache block size is at or above the target IO size every run holds a
//! single block and the behaviour is exactly that of the pre-BlockRun code. Small
//! block sizes produce longer runs, which is the point: the block stays the unit
//! of cache state while the transfer size stays sane.
//!
//! Lifetime: the RAM buffer is released and the run deleted when the last of its
//! blocks is freed (m_n_live drops to zero), so a run outlives its response
//! handler for as long as any of its blocks is still queued for writing. Code
//! that iterates a run's blocks while completing them must snapshot m_blocks
//! first -- completing the last block destroys the run.
//------------------------------------------------------------------------------

class BlockRun
{
public:
   File               *m_file;
   IO                 *m_io;            // IO that handled current request, used for == / != comparisons only
   void               *m_req_id;        // Identity of requestor -- used for stats.

   char               *m_buff;          // one Cache::RequestRAM(m_alloc_size)
   long long           m_offset;        // file offset of the first block
   int                 m_alloc_size;    // RAM allocated; also the length passed to
                                        // pgRead, which wants whole 4k pages
   int                 m_data_size;     // bytes of file data the run covers; the
                                        // length passed to Read and the expected
                                        // response size. <= m_alloc_size.
   int                 m_first_idx;     // index of the first block
   int                 m_n_live;        // blocks still referencing m_buff

   bool                m_prefetch_run;  // issued by the prefetch thread; drives
                                        // IO::m_active_prefetches accounting.
                                        // Distinct from Block::m_prefetch, which
                                        // says nobody asked for that block's data.
   bool                m_req_cksum_net;
   vCkSum_t            m_cksum_vec;
   int                 m_n_cksum_errors;

   std::vector<Block*> m_blocks;

   BlockRun(File *f, IO *io, void *rid, char *buf, long long off,
            int alloc_size, int data_size, int first_idx, bool prefetch, bool cks_net) :
      m_file(f), m_io(io), m_req_id(rid),
      m_buff(buf), m_offset(off), m_alloc_size(alloc_size), m_data_size(data_size),
      m_first_idx(first_idx), m_n_live(0),
      m_prefetch_run(prefetch), m_req_cksum_net(cks_net), m_n_cksum_errors(0)
   {}

   char*     get_buff()       const { return m_buff;      }
   long long get_offset()     const { return m_offset;    }
   int       get_alloc_size() const { return m_alloc_size; }
   int       get_data_size()  const { return m_data_size;  }
   int       get_n_blocks()   const { return (int) m_blocks.size(); }

   File* get_file()   const { return m_file;   }
   IO*   get_io()     const { return m_io;     }
   void* get_req_id() const { return m_req_id; }

   void reset_error_and_set_io(IO *io, void *rid)
   {
      for (Block *b : m_blocks) b->reset_error();
      m_io     = io;
      m_req_id = rid;
   }

   bool      req_cksum_net() const { return m_req_cksum_net; }
   bool      has_cksums()    const { return ! m_cksum_vec.empty(); }
   vCkSum_t& ref_cksum_vec()       { return m_cksum_vec; }
   int       get_n_cksum_errors()  { return m_n_cksum_errors; }
   int*      ptr_n_cksum_errors()  { return &m_n_cksum_errors; }
};

using BlockRunList_t = std::vector<BlockRun*>;

inline File* Block::get_file()       const { return m_run->m_file;          }
inline IO*   Block::get_io()         const { return m_run->m_io;            }
inline void* Block::get_req_id()     const { return m_run->m_req_id;        }
inline bool  Block::req_cksum_net()  const { return m_run->m_req_cksum_net; }

// ================================================================

//------------------------------------------------------------------------------
//! Completion of one remote request covering a single run.
//------------------------------------------------------------------------------

class BlockRunResponseHandler : public XrdOucCacheIOCB
{
public:
   BlockRun *m_run;

   BlockRunResponseHandler(BlockRun *r) : m_run(r) {}

   void Done(int result) override;
};

// ----------------------------------------------------------------

//------------------------------------------------------------------------------
//! Completion of one remote ReadV or pgReadV covering several runs. The origin
//! gives us a single result for the whole vector, so all runs succeed or fail
//! together.
//!
//! For the pgReadV case the handler also owns the checksum vectors, because
//! XrdOucCacheIO::pgReadV wants them as one contiguous array and the runs' own
//! vectors are separate objects. They are moved into the runs on completion,
//! before any of them can reach the disk.
//------------------------------------------------------------------------------

class BlockSequenceResponseHandler : public XrdOucCacheIOCB
{
public:
   File           *m_file;
   BlockRunList_t  m_runs;
   int             m_expected_size;

   std::vector<vCkSum_t> m_cksum_vecs;      // non-empty only for pgReadV
   int                   m_n_cksum_errors = 0;

   BlockSequenceResponseHandler(File *f, BlockRunList_t runs, int expected_size,
                                bool with_cksums = false) :
      m_file(f), m_runs(std::move(runs)), m_expected_size(expected_size)
   {
      if (with_cksums) m_cksum_vecs.resize(m_runs.size());
   }

   bool       req_cksum_net()      const { return ! m_cksum_vecs.empty(); }
   vCkSum_t*  ptr_cksum_vecs()           { return m_cksum_vecs.data(); }
   int*       ptr_n_cksum_errors()       { return &m_n_cksum_errors; }

   void Done(int result) override;
};

// ----------------------------------------------------------------

class DirectResponseHandler : public XrdOucCacheIOCB
{
public:
   XrdSysMutex   m_mutex;
   File         *m_file;
   ReadRequest  *m_read_req;
   int           m_to_wait;
   int           m_bytes_read = 0;
   int           m_errno = 0;

   DirectResponseHandler(File *file, ReadRequest *rreq, int to_wait) :
      m_file(file), m_read_req(rreq), m_to_wait(to_wait)
   {}

   void Done(int result) override;
};

// ================================================================

class File
{
   friend class Cache;
   friend class BlockRunResponseHandler;
   friend class BlockSequenceResponseHandler;
   friend class DirectResponseHandler;
public:
   // Constructor, destructor, Open() and Close() are private.

   //! Static constructor that also does Open. Returns null ptr if Open fails.
   static File* FileOpen(const std::string &path, long long offset, long long fileSize, XrdOucCacheIO *inputIO);

   //! Handle removal of a block run from Cache's write queue.
   void RunRemovedFromWriteQ(BlockRun*);

   //! Handle removal of a set of block runs from Cache's write queue.
   void RunsRemovedFromWriteQ(std::list<BlockRun*>&);

   //! Normal read.
   int Read(IO *io, char* buff, long long offset, int size, ReadReqRH *rh);

   //! Vector read.
   int ReadV(IO *io, const XrdOucIOVec *readV, int readVnum, ReadReqRH *rh);

   //----------------------------------------------------------------------
   //! \brief Notification from IO that it has been updated (remote open).
   //----------------------------------------------------------------------
   void ioUpdated(IO *io);

   //----------------------------------------------------------------------
   //! \brief Initiate close. Return true if still IO active.
   //! Used in XrdPosixXrootd::Close()
   //----------------------------------------------------------------------
   bool ioActive(IO *io);

   //----------------------------------------------------------------------
   //! \brief Flags that detach stats should be written out in final sync.
   //! Called from CacheIO upon Detach.
   //----------------------------------------------------------------------
   void RequestSyncOfDetachStats();

   //----------------------------------------------------------------------
   //! \brief Returns true if any of blocks need sync.
   //! Called from Cache::dec_ref_cnt on zero ref cnt
   //----------------------------------------------------------------------
   bool FinalizeSyncBeforeExit();

   //----------------------------------------------------------------------
   //! Sync file cache inf o and output data with disk
   //----------------------------------------------------------------------
   void Sync();

   void WriteRunToDisk(BlockRun *run);

   void Prefetch();

   float GetPrefetchScore() const;

   //! Log path
   const char* lPath() const;

   const std::string& GetLocalPath() const { return m_filename; }

   XrdSysError* GetLog() const;
   XrdSysTrace* GetTrace() const;

   long long GetFileSize() const { return m_file_size; }

   void AddIO(IO *io);
   int  GetPrefetchCountOnIO(IO *io);
   void StopPrefetchingOnIO(IO *io);
   void RemoveIO(IO *io);

   std::string        GetRemoteLocations()   const;
   const Info::AStat* GetLastAccessStats()   const { return m_cfi.GetLastAccessStats(); }
   size_t             GetAccessCnt()         const { return m_cfi.GetAccessCnt(); }
   int                GetBlockSize()         const { return m_cfi.GetBufferSize(); }
   int                GetNBlocks()           const { return m_cfi.GetNBlocks(); }
   int                GetNDownloadedBlocks() const { return m_cfi.GetNDownloadedBlocks(); }
   long long          GetPrefetchedBytes()   const { return m_prefetch_bytes; }
   const Stats&       RefStats()             const { return m_stats; }

   int Fstat(struct stat &sbuff);

   // These three methods are called under Cache's m_active lock
   int get_ref_cnt() { return   m_ref_cnt; }
   int inc_ref_cnt() { return ++m_ref_cnt; }
   int dec_ref_cnt() { return --m_ref_cnt; }

   long long initiate_emergency_shutdown();
   bool      is_in_emergency_shutdown() { return m_in_shutdown; }

private:
   //! Constructor.
   File(const std::string &path, long long offset, long long fileSize);

   //! Destructor.
   ~File();

   //! Close data and cinfo file.
   void Close();

   //! Open file handle for data file and info file on local disk.
   bool Open(XrdOucCacheIO *inputIO);
   void parse_pfc_url_args(XrdOucCacheIO *inputIO, long long &pfc_blocksize, int &pfc_prefetch) const;

   static const char *m_traceID;

   int            m_ref_cnt;            //!< number of references from IO or sync

   XrdOssDF      *m_data_file;          //!< file handle for data file on disk
   XrdOssDF      *m_info_file;          //!< file handle for data-info file on disk
   Info           m_cfi;                //!< download status of file blocks and access statistics

   const std::string    m_filename;     //!< filename of data file on disk
   const long long      m_offset;       //!< offset of cached file for block-based / hdfs operation
   const long long      m_file_size;    //!< size of cached disk file for block-based operation

   // IO objects attached to this file.

   typedef std::set<IO*>     IoSet_t;
   typedef IoSet_t::iterator IoSet_i;

   IoSet_t    m_io_set;
   IoSet_i    m_current_io;     //!< IO object to be used for prefetching.
   int        m_ios_in_detach;  //!< Number of IO objects to which we replied false to ioActive() and will be removed soon.

   // FSync

   std::vector<int>  m_writes_during_sync;
   long long m_bytes_during_sync;   //!< bytes written while a sync was in progress
   long long m_non_flushed_bytes;   //!< bytes written to disk since the last sync
   bool m_in_sync;
   bool m_detach_time_logged;
   bool m_in_shutdown;        //!< file is in emergency shutdown due to irrecoverable error or unlink request

   // Block state and management

   typedef std::list<int>        IntList_t;
   typedef IntList_t::iterator   IntList_i;

   typedef std::map<int, Block*> BlockMap_t;
   typedef BlockMap_t::iterator  BlockMap_i;

   BlockMap_t    m_block_map;
   XrdSysCondVar m_state_cond;
   long long     m_block_size;
   int           m_num_blocks;
   int           m_max_run_blocks;     //!< max blocks per BlockRun, from pfc.iosize
   int           m_max_gap_blocks;     //!< max blocks bridged when grouping a run, from pfc.iogap

   // Stats and ResourceMonitor interface

   Stats         m_stats;              //!< cache statistics for this instance
   Stats         m_delta_stats;        //!< unreported updates to stats
   long long     m_st_blocks;          //!< last reported st_blocks
   long long     m_resmon_report_threshold;
   int           m_resmon_token;       //!< token used in communication with the ResourceMonitor

   void check_delta_stats();
   void report_and_merge_delta_stats();

   std::set<std::string> m_remote_locations; //!< Gathered in AddIO / ioUpdate / ioActive.
   void insert_remote_location(const std::string &loc);

   // Prefetch

   enum PrefetchState_e { kOff=-1, kOn, kHold, kStopped, kComplete };

   PrefetchState_e m_prefetch_state;
   int             m_prefetch_max_blocks_in_flight;

   long long m_prefetch_bytes;
   int   m_prefetch_read_cnt;
   int   m_prefetch_hit_cnt;
   float m_prefetch_score;              // cached

   void inc_prefetch_read_cnt(int prc) { if (prc) { m_prefetch_read_cnt += prc; calc_prefetch_score(); } }
   void inc_prefetch_hit_cnt (int phc) { if (phc) { m_prefetch_hit_cnt  += phc; calc_prefetch_score(); } }
   void calc_prefetch_score() { m_prefetch_score = float(m_prefetch_hit_cnt) / m_prefetch_read_cnt; }

   // Helpers

   bool overlap(int blk,               // block to query
                long long blk_size,    //
                long long req_off,     // offset of user request
                int req_size,          // size of user request
                // output:
                long long &off,        // offset in user buffer
                long long &blk_off,    // offset in block
                int       &size);

   // Read & ReadV

   //! Allocate a run of n_blocks consecutive blocks starting at first_idx.
   //! Returns null if RAM is not available. Must be called under m_state_cond.
   BlockRun* PrepareBlockRun(int first_idx, int n_blocks, IO *io, void *req_id, bool prefetch);

   void   ProcessRunRequest (BlockRun       *run);
   void   ProcessRunRequests(BlockRunList_t& runs, IO *io);

   void   RequestBlocksDirect(IO *io, ReadRequest *read_req, std::vector<XrdOucIOVec>& ioVec, int expected_size);

   int    ReadBlocksFromDisk(std::vector<XrdOucIOVec>& ioVec, int expected_size);

   int    ReadOpusCoalescere(IO *io, const XrdOucIOVec *readV, int readVnum,
                             ReadReqRH *rh, const char *tpfx);

   void ProcessDirectReadFinished(ReadRequest *rreq, int bytes_read, int error_cond);
   void ProcessBlockError(Block *b, ReadRequest *rreq);
   void ProcessBlockSuccess(Block *b, ChunkRequest &creq);
   void FinalizeReadRequest(ReadRequest *rreq);

   void ProcessRunResponse(BlockRun *run, int res);
   void ProcessSequenceResponse(BlockSequenceResponseHandler *h, int res);

   // Block management

   void inc_ref_count(Block* b);
   void dec_ref_count(Block* b, int count = 1);
   void free_block(Block*);

   //! Release the reference PrepareBlockRun took on each block on the run's own
   //! behalf. Always called under lock; may free blocks and delete the run.
   void release_run_ref(BlockRun *run);

   bool select_current_io_or_disable_prefetching(bool skip_current);

   int  offsetIdx(int idx) const;
};

//------------------------------------------------------------------------------

inline void File::inc_ref_count(Block* b)
{
   // Method always called under lock.
   b->m_refcnt++;
}

//------------------------------------------------------------------------------

inline void File::dec_ref_count(Block* b, int count)
{
   // Method always called under lock.
   assert(b->is_finished());
   b->m_refcnt -= count;
   assert(b->m_refcnt >= 0);

   if (b->m_refcnt == 0)
   {
      free_block(b);
   }
}

}

#endif
