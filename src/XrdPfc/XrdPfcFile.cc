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


#include "XrdPfcFile.hh"
#include "XrdPfc.hh"
#include "XrdPfcResourceMonitor.hh"
#include "XrdPfcIO.hh"
#include "XrdPfcTrace.hh"

#include "XProtocol/XProtocol.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucJson.hh"
#include "XrdSfs/XrdSfsInterface.hh"

#include "XrdCl/XrdClURL.hh"
#include "XrdCl/XrdClFileStateHandler.hh"

#include <cassert>
#include <cstdio>
#include <sstream>
#include <unordered_map>

#include <fcntl.h>

using namespace XrdPfc;

namespace
{

const int BLOCK_WRITE_MAX_ATTEMPTS = 4;

Cache* cache() { return &Cache::GetInstance(); }

}

const char *File::m_traceID = "File";

//------------------------------------------------------------------------------

File::File(const std::string& path, long long iOffset, long long iFileSize) :
   m_ref_cnt(0),
   m_data_file(0),
   m_info_file(0),
   m_cfi(Cache::TheOne().GetTrace(), Cache::TheOne().is_prefetch_enabled()),
   m_filename(path),
   m_offset(iOffset),
   m_file_size(iFileSize),
   m_current_io(m_io_set.end()),
   m_ios_in_detach(0),
   m_bytes_during_sync(0),
   m_non_flushed_bytes(0),
   m_in_sync(false),
   m_detach_time_logged(false),
   m_in_shutdown(false),
   m_state_cond(0),
   m_block_size(0),
   m_num_blocks(0),
   m_max_run_blocks(1),
   m_resmon_token(-1),
   m_prefetch_state(kOff),
   m_prefetch_bytes(0),
   m_prefetch_read_cnt(0),
   m_prefetch_hit_cnt(0),
   m_prefetch_score(0)
{}

File::~File()
{
   TRACEF(Debug, "~File() for ");
}

void File::Close()
{
   // Close is called while nullptr is put into Cache::m_active map, see Cache::dec_ref_count(File*).
   // A stat is called after close to re-check that m_stat_blocks have been reported correctly
   // to the resource-monitor. Note that the reporting is already clamped down to m_file_size
   // in report_and_merge_delta_stats() below.
   //
   // XFS can pre-allocate significant amount of blocks (1 GB at 1GB mark, 4 GB above 4GB) and those
   // get reported in as stat.st_blocks.
   // The reported number is correct in a stat immediately following a close.
   // If one starts off by writing the last byte of the file, this pre-allocation does not get
   // triggered up to that point. But comes back with a vengeance right after.
   //
   // To be determined if other FSes do something similar (Ceph, ZFS, ...). Ext4 doesn't.

   if (m_info_file)
   {
      TRACEF(Debug, "Close() closing info-file ");
      m_info_file->Close();
      delete m_info_file;
      m_info_file = nullptr;
   }

   if (m_data_file)
   {
      TRACEF(Debug, "Close() closing data-file ");
      m_data_file->Close();
      delete m_data_file;
      m_data_file = nullptr;
   }

   if (m_resmon_token >= 0)
   {
      // Last update of file stats has been sent from the final Sync unless we are in_shutdown --
      // but in this case the file will get unlinked by the cache and reported as purge event.
      // We check if the reported st_blocks so far is correct.
      if (m_stats.m_BytesWritten > 0 && ! m_in_shutdown) {
         struct stat s;
         int sr = Cache::GetInstance().GetOss()->Stat(m_filename.c_str(), &s);
         if (sr == 0 && s.st_blocks != m_st_blocks) {
            Stats stats;
            stats.m_StBlocksAdded = s.st_blocks - m_st_blocks;
            m_st_blocks = s.st_blocks;
            Cache::ResMon().register_file_update_stats(m_resmon_token, stats);
         }
      }

      Cache::ResMon().register_file_close(m_resmon_token, time(0), m_stats);
   }

   TRACEF(Debug, "Close() finished, prefetch score = " <<  m_prefetch_score);
}

//------------------------------------------------------------------------------

File* File::FileOpen(const std::string &path, long long offset, long long fileSize, XrdOucCacheIO *inputIO)
{
   File *file = new File(path, offset, fileSize);
   if ( ! file->Open(inputIO))
   {
      delete file;
      file = 0;
   }
   return file;
}

//------------------------------------------------------------------------------

long long File::initiate_emergency_shutdown()
{
   // Called from Cache::Unlink() when the file is currently open.
   // Cache::Unlink is also called on FSync error and when wrong number of bytes
   // is received from a remote read.
   //
   // From this point onward the file will not be written to, cinfo file will
   // not be updated, and all new read requests will return -ENOENT.
   //
   // File's entry in the Cache's active map is set to nullptr and will be
   // removed from there shortly, in any case, well before this File object
   // shuts down. Cache::Unlink() also reports the appropriate purge event.

   XrdSysCondVarHelper _lck(m_state_cond);

   m_in_shutdown = true;

   if (m_prefetch_state != kStopped && m_prefetch_state != kComplete)
   {
      m_prefetch_state = kStopped;
      cache()->DeRegisterPrefetchFile(this);
   }

   report_and_merge_delta_stats();

   return m_st_blocks;
}

//------------------------------------------------------------------------------

void File::check_delta_stats()
{
   // Called under m_state_cond lock.
   // BytesWritten indirectly trigger an unconditional merge through periodic Sync().
   if (m_delta_stats.BytesReadAndWritten() >= m_resmon_report_threshold && ! m_in_shutdown)
      report_and_merge_delta_stats();
}

void File::report_and_merge_delta_stats()
{
   // Called under m_state_cond lock.
   struct stat s;
   m_data_file->Fstat(&s);
   // Do not report st_blocks beyond 4kB round-up over m_file_size. Some FSs report
   // aggressive pre-allocation in this field (XFS, 4GB).
   long long max_st_blocks_to_report = (m_file_size & 0xfff) ? ((m_file_size >> 12) + 1) << 3
                                                             :   m_file_size >> 9;
   long long st_blocks_to_report = std::min((long long) s.st_blocks, max_st_blocks_to_report);
   m_delta_stats.m_StBlocksAdded = st_blocks_to_report - m_st_blocks;
   m_st_blocks = st_blocks_to_report;
   Cache::ResMon().register_file_update_stats(m_resmon_token, m_delta_stats);
   m_stats.AddUp(m_delta_stats);
   m_delta_stats.Reset();
}

//------------------------------------------------------------------------------

void File::RunRemovedFromWriteQ(BlockRun* run)
{
   TRACEF(Dump, "RunRemovedFromWriteQ() run = " << (void*) run << " idx= " << run->m_first_idx
          << " n_blocks= " << run->get_n_blocks());

   XrdSysCondVarHelper _lck(m_state_cond);
   for (Block *b : run->m_blocks)
      dec_ref_count(b);
}

void File::RunsRemovedFromWriteQ(std::list<BlockRun*>& runs)
{
   TRACEF(Dump, "RunsRemovedFromWriteQ() n_runs = " << runs.size());

   XrdSysCondVarHelper _lck(m_state_cond);

   for (BlockRun *run : runs)
      for (Block *b : run->m_blocks)
         dec_ref_count(b);
}

//------------------------------------------------------------------------------

void File::ioUpdated(IO *io)
{
   std::string loc(io->GetLocation());
   XrdSysCondVarHelper _lck(m_state_cond);
   insert_remote_location(loc);
}

//------------------------------------------------------------------------------

bool File::ioActive(IO *io)
{
   // Returns true if delay is needed.

   TRACEF(Debug, "ioActive start for io " << io);

   std::string loc(io->GetLocation());

   {
      XrdSysCondVarHelper _lck(m_state_cond);

      IoSet_i mi = m_io_set.find(io);

      if (mi != m_io_set.end())
      {
         unsigned int n_active_reads = io->m_active_read_reqs;

         TRACE(Info, "ioActive for io " << io <<
                ", active_reads "       << n_active_reads <<
                ", active_prefetches "  << io->m_active_prefetches <<
                ", allow_prefetching "  << io->m_allow_prefetching <<
                ", ios_in_detach "      << m_ios_in_detach);
         TRACEF(Info,
                "\tio_map.size() "      << m_io_set.size() <<
                ", block_map.size() "   << m_block_map.size() << ", file");

         insert_remote_location(loc);

         io->m_allow_prefetching = false;
         io->m_in_detach = true;

         // Check if any IO is still available for prfetching. If not, stop it.
         if (m_prefetch_state == kOn || m_prefetch_state == kHold)
         {
            if ( ! select_current_io_or_disable_prefetching(false) )
            {
               TRACEF(Debug, "ioActive stopping prefetching after io " << io << " retreat.");
            }
         }

         // On last IO, consider write queue blocks. Note, this also contains
         // blocks being prefetched.

         bool io_active_result;

         if (n_active_reads > 0)
         {
            io_active_result = true;
         }
         else if (m_io_set.size() - m_ios_in_detach == 1)
         {
            io_active_result = ! m_block_map.empty();
         }
         else
         {
            io_active_result = io->m_active_prefetches > 0;
         }

         if ( ! io_active_result)
         {
            ++m_ios_in_detach;
         }

         TRACEF(Info, "ioActive for io " << io << " returning " << io_active_result << ", file");

         return io_active_result;
      }
      else
      {
         TRACEF(Error, "ioActive io " << io <<" not found in IoSet. This should not happen.");
         return false;
      }
   }
}

//------------------------------------------------------------------------------

void File::RequestSyncOfDetachStats()
{
   XrdSysCondVarHelper _lck(m_state_cond);
   m_detach_time_logged = false;
}

bool File::FinalizeSyncBeforeExit()
{
   // Returns true if sync is required.
   // This method is called after corresponding IO is detached from PosixCache.

   XrdSysCondVarHelper _lck(m_state_cond);
   if ( ! m_in_shutdown)
   {
     if ( ! m_writes_during_sync.empty() || m_non_flushed_bytes > 0 || ! m_detach_time_logged)
     {
       report_and_merge_delta_stats();
       m_cfi.WriteIOStatDetach(m_stats);
       m_detach_time_logged = true;
       m_in_sync            = true;
       TRACEF(Debug, "FinalizeSyncBeforeExit requesting sync to write detach stats");
       return true;
     }
   }
   TRACEF(Debug, "FinalizeSyncBeforeExit sync not required");
   return false;
}

//------------------------------------------------------------------------------

void File::AddIO(IO *io)
{
   // Called from Cache::GetFile() when a new IO asks for the file.

   TRACEF(Debug, "AddIO() io = " << (void*)io);

   time_t      now = time(0);
   std::string loc(io->GetLocation());

   m_state_cond.Lock();

   IoSet_i mi = m_io_set.find(io);

   if (mi == m_io_set.end())
   {
      m_io_set.insert(io);
      io->m_attach_time = now;
      m_delta_stats.IoAttach();

      insert_remote_location(loc);

      if (m_prefetch_state == kStopped)
      {
         m_prefetch_state = kOn;
         cache()->RegisterPrefetchFile(this);
      }
   }
   else
   {
      TRACEF(Error, "AddIO() io = " << (void*)io << " already registered.");
   }

   m_state_cond.UnLock();
}

//------------------------------------------------------------------------------

void File::RemoveIO(IO *io)
{
   // Called from Cache::ReleaseFile.

   TRACEF(Debug, "RemoveIO() io = " << (void*)io);

   time_t now = time(0);

   m_state_cond.Lock();

   IoSet_i mi = m_io_set.find(io);

   if (mi != m_io_set.end())
   {
      if (mi == m_current_io)
      {
         ++m_current_io;
      }

      m_delta_stats.IoDetach(now - io->m_attach_time);
      m_io_set.erase(mi);
      --m_ios_in_detach;

      if (m_io_set.empty() && m_prefetch_state != kStopped && m_prefetch_state != kComplete)
      {
         TRACEF(Error, "RemoveIO() io = " << (void*)io << " Prefetching is not stopped/complete -- it should be by now.");
         m_prefetch_state = kStopped;
         cache()->DeRegisterPrefetchFile(this);
      }
   }
   else
   {
      TRACEF(Error, "RemoveIO() io = " << (void*)io << " is NOT registered.");
   }

   m_state_cond.UnLock();
}

//------------------------------------------------------------------------------

bool File::Open(XrdOucCacheIO *inputIO)
{
   // Sets errno accordingly.

   static const char *tpfx = "Open() ";

   TRACEF(Dump, tpfx << "entered");

   // Before touching anything, check with ResourceMonitor if a scan is in progress.
   // This function will wait internally if needed until it is safe to proceed.
   Cache::ResMon().CrossCheckIfScanIsInProgress(m_filename, m_state_cond);

   const Configuration &conf = Cache::GetInstance().RefConfiguration();

   XrdOss     &myOss  = * Cache::GetInstance().GetOss();
   const char *myUser =   conf.m_username.c_str();
   XrdOucEnv   myEnv;
   struct stat data_stat, info_stat;

   std::string ifn = m_filename + Info::s_infoExtension;

   bool data_existed = (myOss.Stat(m_filename.c_str(), &data_stat) == XrdOssOK);
   bool info_existed = (myOss.Stat(ifn.c_str(),        &info_stat) == XrdOssOK);

   // Create the data file itself.
   char size_str[32]; sprintf(size_str, "%lld", m_file_size);
   myEnv.Put("oss.asize",  size_str);
   myEnv.Put("oss.cgroup", conf.m_data_space.c_str());

   int res;

   if ((res = myOss.Create(myUser, m_filename.c_str(), 0600, myEnv, XRDOSS_mkpath)) != XrdOssOK)
   {
      TRACEF(Error, tpfx << "Create failed " << ERRNO_AND_ERRSTR(-res));
      errno = -res;
      return false;
   }

   m_data_file = myOss.newFile(myUser);
   if ((res = m_data_file->Open(m_filename.c_str(), O_RDWR, 0600, myEnv)) != XrdOssOK)
   {
      TRACEF(Error, tpfx << "Open failed " << ERRNO_AND_ERRSTR(-res));
      errno = -res;
      delete m_data_file; m_data_file = 0;
      return false;
   }

   myEnv.Put("oss.asize", "64k"); // Advisory, block-map and access list lengths vary.
   myEnv.Put("oss.cgroup", conf.m_meta_space.c_str());
   if ((res = myOss.Create(myUser, ifn.c_str(), 0600, myEnv, XRDOSS_mkpath)) != XrdOssOK)
   {
      TRACE(Error, tpfx << "Create failed for info file " << ifn << ERRNO_AND_ERRSTR(-res));
      errno = -res;
      m_data_file->Close(); delete m_data_file; m_data_file = 0;
      return false;
   }

   m_info_file = myOss.newFile(myUser);
   if ((res = m_info_file->Open(ifn.c_str(), O_RDWR, 0600, myEnv)) != XrdOssOK)
   {
      TRACEF(Error, tpfx << "Failed for info file " << ifn  << ERRNO_AND_ERRSTR(-res));
      errno = -res;
      delete m_info_file; m_info_file = 0;
      m_data_file->Close(); delete m_data_file;   m_data_file   = 0;
      return false;
   }

   bool initialize_info_file = true;

   if (info_existed && m_cfi.Read(m_info_file, ifn.c_str()))
   {
      TRACEF(Debug, tpfx << "Reading existing info file. (data_existed=" << data_existed  <<
             ", data_size_stat=" << (data_existed ? data_stat.st_size : -1ll) <<
             ", data_size_from_last_block=" << m_cfi.GetExpectedDataFileSize() <<
             ", block_size=" << (m_cfi.GetBufferSize() >> 10) << "k)");

      // Check if data file exists and is of reasonable size.
      if (data_existed && data_stat.st_size >= m_cfi.GetExpectedDataFileSize())
      {
         initialize_info_file = false;
      } else {
         TRACEF(Warning, tpfx << "Basic sanity checks on data file failed, resetting info file, truncating data file.");
         m_cfi.ResetAllAccessStats();
         m_data_file->Ftruncate(0);
         // data-file might not have existed at entry -- data_stat is then undefined
         if (data_existed)
            Cache::ResMon().register_file_purge(m_filename, data_stat.st_blocks);
      }
   }

   if ( ! initialize_info_file && m_cfi.GetCkSumState() != conf.get_cs_Chk())
   {
      if (conf.does_cschk_have_missing_bits(m_cfi.GetCkSumState()) &&
          conf.should_uvkeep_purge(time(0) - m_cfi.GetNoCkSumTimeForUVKeep()))
      {
         TRACEF(Info, tpfx << "Cksum state of file insufficient, uvkeep test failed, resetting info file, truncating data file.");
         initialize_info_file = true;
         m_cfi.ResetAllAccessStats();
         m_data_file->Ftruncate(0);
         // data-file is known to exist due to checks in the previous if block
         Cache::ResMon().register_file_purge(m_filename, data_stat.st_blocks);
      } else {
         // TODO: If the file is complete, we don't need to reset net cksums.
         m_cfi.DowngradeCkSumState(conf.get_cs_Chk());
      }
   }

   // Check if we have pfc url arguments.
   long long pfc_blocksize = conf.m_bufferSize;
   int       pfc_prefetch  = conf.m_prefetch_max_blocks;
   if (conf.m_cgi_blocksize_allowed || conf.m_cgi_prefetch_allowed)
   {
      parse_pfc_url_args(inputIO, pfc_blocksize, pfc_prefetch);
   }

   if (initialize_info_file)
   {
      m_cfi.SetBufferSizeFileSizeAndCreationTime(pfc_blocksize, m_file_size);
      m_cfi.SetCkSumState(conf.get_cs_Chk());
      m_cfi.ResetNoCkSumTime();
      m_cfi.Write(m_info_file, ifn.c_str());
      m_info_file->Fsync();
      cache()->WriteFileSizeXAttr(m_info_file->getFD(), m_file_size);

      if (cache()->RefConfiguration().m_httpcc)
      {
         std::string  responseFctl;
         int resFctl = inputIO->Fcntl(XrdOucCacheOp::Code::QFinfo, "head", responseFctl);
         if (resFctl == 0)
         {
            std::string cc_str = responseFctl;
            nlohmann::json cc_json =  nlohmann::json::parse(cc_str);
            if (cc_json.contains("max-age"))
            {
               time_t ma = cc_json["max-age"];
               ma += time(NULL);
               cc_json["expire"] = ma;
               cc_str = cc_json.dump();
            }
            TRACE(Error, "GetFile() XrdCl::File::Fcntl value " << cc_str);
            cache()->WriteCacheControlXAttr(m_info_file->getFD(), nullptr, cc_str);
         }
         else if (resFctl != kXR_Unsupported)
         {
            TRACE(Error, "GetFile() XrdCl::File::Fcntl query XrdCl::QueryCode::FInfo failed " << inputIO->Path());
         }
      }

      TRACEF(Debug, tpfx << "Creating new file info, data size = " <<  m_file_size << 
                            " num blocks = "  << m_cfi.GetNBlocks() <<
                            " block size = " << pfc_blocksize);
   }
   else
   {
      if (futimens(m_info_file->getFD(), NULL)) {
         TRACEF(Error, tpfx << "failed setting modification time " << ERRNO_AND_ERRSTR(errno));
      }
      if (pfc_blocksize != conf.m_bufferSize) {
         TRACEF(Info, tpfx << "URL CGI pfc.blocksize ignored for an already existing file");
      }
   }

   m_cfi.WriteIOStatAttach();
   m_state_cond.Lock();
   m_block_size = m_cfi.GetBufferSize();
   m_num_blocks = m_cfi.GetNBlocks();
   // A BlockRun is one remote request and one disk write. Grow runs up to the
   // configured target IO size; at or above it every run is a single block.
   m_max_run_blocks = (int) std::max(1ll, conf.m_iosize / m_block_size);
   m_prefetch_state = (m_cfi.IsComplete()) ? kComplete : kStopped; // Will engage in AddIO().
   m_prefetch_max_blocks_in_flight = pfc_prefetch;
   if (pfc_prefetch != conf.m_prefetch_max_blocks)
      TRACEF(Debug, tpfx << "pfc.prefetch set to " << pfc_prefetch << " via CGI parameter");

   m_data_file->Fstat(&data_stat);
   m_st_blocks = data_stat.st_blocks;

   m_resmon_token = Cache::ResMon().register_file_open(m_filename, time(0), data_existed);
   constexpr long long MB = 1024 * 1024;
   m_resmon_report_threshold = std::min(std::max(10 * MB, m_file_size / 20), 500 * MB);
   // m_resmon_report_threshold_scaler; // something like 10% of original threshold, to adjust
   // actual threshold based on return values from register_file_update_stats().

   m_state_cond.UnLock();

   return true;
}

void File::parse_pfc_url_args(XrdOucCacheIO* inputIO, long long &pfc_blocksize, int &pfc_prefetch) const
{
   const Configuration &conf = Cache::TheOne().RefConfiguration();

   XrdCl::URL url(inputIO->Path());
   auto const & urlp = url.GetParams();

   auto extract = [&](const std::string &key, std::string &value) -> bool {
      auto it = urlp.find(key);
      if (it != urlp.end()) {
         value = it->second;
         return true;
      } else {
         value.clear();
         return false;
      }
   };

   std::string val;
   if (conf.m_cgi_blocksize_allowed && extract("pfc.blocksize", val))
   {
      const char *tpfx = "File::Open::urlcgi pfc.blocksize ";
      long long bsize;
      if (Cache::TheOne().blocksize_str2value(tpfx, val.c_str(), bsize,
                                              conf.m_cgi_min_bufferSize, conf.m_cgi_max_bufferSize))
      {
         pfc_blocksize = bsize;
      } else {
         TRACEF(Error, tpfx << "Error processing the parameter.");
      }
   }
   if (conf.m_cgi_prefetch_allowed && extract("pfc.prefetch", val))
   {
      const char *tpfx = "File::Open::urlcgi pfc.prefetch ";
      int pref;
      if (Cache::TheOne().prefetch_str2value(tpfx, val.c_str(), pref,
                                             conf.m_cgi_min_prefetch_max_blocks, conf.m_cgi_max_prefetch_max_blocks))
      {
         pfc_prefetch = pref;
      } else {
         TRACEF(Error, tpfx << "Error processing the parameter.");
      }
   }
}

//------------------------------------------------------------------------------

int File::Fstat(struct stat &sbuff)
{
   // Stat on an open file.
   // Corrects size to actual full size of the file.
   // Sets atime to 0 if the file is only partially downloaded, in accordance
   // with pfc.onlyifcached settings.
   // Called from IO::Fstat() and Cache::Stat() when the file is active.
   // Returns 0 on success, -errno on error.

   int res;

   if ((res = m_data_file->Fstat(&sbuff))) return res;

   sbuff.st_size = m_file_size;

   bool is_cached = cache()->DecideIfConsideredCached(m_file_size, sbuff.st_blocks * 512ll);
   if ( ! is_cached)
      sbuff.st_atime = 0;

   return 0;
}

//==============================================================================
// Read and helpers
//==============================================================================

bool File::overlap(int blk,            // block to query
                   long long blk_size, //
                   long long req_off,  // offset of user request
                   int req_size,       // size of user request
                   // output:
                   long long &off,     // offset in user buffer
                   long long &blk_off, // offset in block
                   int       &size)    // size to copy
{
   const long long beg     = blk * blk_size;
   const long long end     = beg + blk_size;
   const long long req_end = req_off + req_size;

   if (req_off < end && req_end > beg)
   {
      const long long ovlp_beg = std::max(beg, req_off);
      const long long ovlp_end = std::min(end, req_end);

      off     = ovlp_beg - req_off;
      blk_off = ovlp_beg - beg;
      size    = (int) (ovlp_end - ovlp_beg);

      assert(size <= blk_size);
      return true;
   }
   else
   {
      return false;
   }
}

//------------------------------------------------------------------------------

BlockRun* File::PrepareBlockRun(int first_idx, int n_blocks, IO *io, void *req_id, bool prefetch)
{
   // Must be called w/ state_cond locked.
   // Checks on size etc should be done before; in particular, none of the
   // blocks [first_idx, first_idx + n_blocks) may be in m_block_map already.
   //
   // Reference counts are 0 so increase them in the calling function if you
   // want to catch the blocks while still in memory.

   const long long off   = (long long) first_idx * m_block_size;
   const int  last_block = m_num_blocks - 1;
   const bool cs_net     = cache()->RefConfiguration().is_cschk_net();

   // Size of the run in the file. Only the very last block of the file can be
   // short, and it can only be the last block of a run.
   int run_size;
   if (first_idx + n_blocks - 1 == last_block)
      run_size = (int) (m_file_size - off);
   else
      run_size = n_blocks * m_block_size;

   // pgRead wants whole pages; round the allocation up over a short final block.
   int alloc_size = run_size;
   if (cs_net && alloc_size & 0xFFF) alloc_size = (alloc_size & ~0xFFF) + 0x1000;

   char *buf = cache()->RequestRAM(alloc_size);
   if ( ! buf)
   {
      TRACEF(Dump, "PrepareBlockRun() idx " << first_idx << " n " << n_blocks <<
             " prefetch " << prefetch << ", RAM allocation failed.");
      return nullptr;
   }

   BlockRun *run = new (std::nothrow) BlockRun(this, io, req_id, buf, off,
                                               alloc_size, run_size, first_idx,
                                               prefetch, cs_net);
   if ( ! run)
   {
      cache()->ReleaseRAM(buf, alloc_size);
      TRACEF(Dump, "PrepareBlockRun() idx " << first_idx << " n " << n_blocks << ", run allocation failed.");
      return nullptr;
   }

   run->m_blocks.reserve(n_blocks);

   for (int k = 0; k < n_blocks; ++k)
   {
      const int       idx      = first_idx + k;
      const long long blk_off  = off + (long long) k * m_block_size;
      const int       blk_size = (idx == last_block) ? (int) (m_file_size - blk_off) : (int) m_block_size;

      Block *b = new (std::nothrow) Block(run, buf + (long long) k * m_block_size,
                                          blk_off, blk_size, prefetch);
      if ( ! b)
      {
         TRACEF(Dump, "PrepareBlockRun() block allocation failed at idx " << idx);
         break;
      }

      run->m_blocks.push_back(b);
      ++run->m_n_live;
      m_block_map[idx] = b;

      // The run holds a reference on each of its blocks for as long as its
      // request is in flight, so that run->m_blocks stays valid while the
      // response is being fanned out. Released in release_run_ref().
      inc_ref_count(b);
   }

   if (run->m_blocks.empty())
   {
      cache()->ReleaseRAM(buf, alloc_size);
      delete run;
      return nullptr;
   }

   // A short run means a Block allocation failed part way; shrink the request to
   // what we can actually accommodate. m_alloc_size stays as allocated, since
   // that is what has to be handed back to ReleaseRAM.
   if (run->get_n_blocks() != n_blocks)
   {
      Block *lb = run->m_blocks.back();
      run->m_data_size = (int) (lb->m_offset + lb->m_size - off);
   }

   // Actual remote request is issued in ProcessRunRequests().

   if (m_prefetch_state == kOn && (int) m_block_map.size() >= m_prefetch_max_blocks_in_flight)
   {
      m_prefetch_state = kHold;
      cache()->DeRegisterPrefetchFile(this);
   }

   return run;
}

void File::ProcessRunRequest(BlockRun *run)
{
   // This *must not* be called with block_map locked.
   // Issues one remote request for the whole run.

   BlockRunResponseHandler* brh = new BlockRunResponseHandler(run);

   if (XRD_TRACE What >= TRACE_Dump) {
      char buf[256];
      snprintf(buf, 256, "idx=%d, n_blocks=%d, run=%p, prefetch=%d, off=%lld, data_size=%d, buff=%p, resp_handler=%p ",
         run->m_first_idx, run->get_n_blocks(), (void*)run, run->m_blocks.front()->m_prefetch,
         run->get_offset(), run->get_data_size(), (void*)run->get_buff(), (void*)brh);
      TRACEF(Dump, "ProcessRunRequest() " << buf);
   }

   if (run->req_cksum_net())
   {
      run->get_io()->GetInput()->pgRead(*brh, run->get_buff(), run->get_offset(), run->get_alloc_size(),
                                        run->ref_cksum_vec(), 0, run->ptr_n_cksum_errors());
   } else {
      run->get_io()->GetInput()->  Read(*brh, run->get_buff(), run->get_offset(), run->get_data_size());
   }
}

void File::ProcessRunRequests(BlockRunList_t& runs, IO *io)
{
   // This *must not* be called with block_map locked.
   //
   // A single run goes out as a plain Read / pgRead. Several runs are batched
   // into one remote ReadV -- unless network checksums are requested, in which
   // case pgRead is mandatory and has no vector form, so we fall back to one
   // pgRead per run.

   if (runs.empty()) return;

   if (runs.size() == 1 || cache()->RefConfiguration().is_cschk_net())
   {
      for (BlockRun *run : runs)
         ProcessRunRequest(run);
      return;
   }

   // One ReadV per XrdProto::maxRvecsz runs. Individual runs are capped well
   // below XrdProto::maxRVdsz by the target IO size, so only the element count
   // can force a split.
   int n_runs = (int) runs.size();
   int pos    = 0;

   while (pos < n_runs)
   {
      const int n = std::min(n_runs - pos, XrdProto::maxRvecsz);

      std::vector<XrdOucIOVec> iov;
      BlockRunList_t           batch;
      iov.reserve(n);
      batch.reserve(n);

      int expected = 0;
      for (int k = 0; k < n; ++k)
      {
         BlockRun *run = runs[pos + k];
         iov.push_back( { run->get_offset(), run->get_data_size(), 0, run->get_buff() } );
         batch.push_back(run);
         expected += run->get_data_size();
      }

      TRACEF(Dump, "ProcessRunRequests() issuing ReadV for n_runs = " << n <<
             ", total_size = " << expected);

      auto *bsh = new BlockSequenceResponseHandler(this, std::move(batch), expected);

      io->GetInput()->ReadV(*bsh, iov.data(), n);

      pos += n;
   }
}

//------------------------------------------------------------------------------

void File::RequestBlocksDirect(IO *io, ReadRequest *read_req, std::vector<XrdOucIOVec>& ioVec, int expected_size)
{
   int n_chunks    = ioVec.size();
   int n_vec_reads = (n_chunks - 1) / XrdProto::maxRvecsz + 1;

   TRACEF(DumpXL, "RequestBlocksDirect() issuing ReadV for n_chunks = " << n_chunks <<
          ", total_size = " << expected_size << ", n_vec_reads = " << n_vec_reads);

   DirectResponseHandler *handler = new DirectResponseHandler(this, read_req, n_vec_reads);

   int pos = 0;
   while (n_chunks > XrdProto::maxRvecsz) {
      io->GetInput()->ReadV( *handler, ioVec.data() + pos, XrdProto::maxRvecsz);
      pos      += XrdProto::maxRvecsz;
      n_chunks -= XrdProto::maxRvecsz;
   }
   io->GetInput()->ReadV( *handler, ioVec.data() + pos, n_chunks);
}

//------------------------------------------------------------------------------

int File::ReadBlocksFromDisk(std::vector<XrdOucIOVec>& ioVec, int expected_size)
{
   TRACEF(DumpXL, "ReadBlocksFromDisk() issuing ReadV for n_chunks = " << (int) ioVec.size() << ", total_size = " << expected_size);

   long long rs = m_data_file->ReadV(ioVec.data(), (int) ioVec.size());

   if (rs < 0)
   {
      TRACEF(Error, "ReadBlocksFromDisk neg retval = " <<  rs);
      return rs;
   }

   if (rs != expected_size)
   {
      TRACEF(Error, "ReadBlocksFromDisk incomplete size = " << rs);
      return -EIO;
   }

   return (int) rs;
}

//------------------------------------------------------------------------------

int File::Read(IO *io, char* iUserBuff, long long iUserOff, int iUserSize, ReadReqRH *rh)
{
   // rrc_func is ONLY called from async processing.
   // If this function returns anything other than -EWOULDBLOCK, rrc_func needs to be called by the caller.
   // This streamlines implementation of synchronous IO::Read().

   TRACEF(Dump, "Read() sid: " << Xrd::hex1 << rh->m_seq_id << " size: " << iUserSize);

   m_state_cond.Lock();

   if (m_in_shutdown || io->m_in_detach)
   {
      m_state_cond.UnLock();
      return m_in_shutdown ? -ENOENT : -EBADF;
   }

   // Shortcut -- file is fully downloaded.

   if (m_cfi.IsComplete())
   {
      m_state_cond.UnLock();
      int ret = m_data_file->Read(iUserBuff, iUserOff, iUserSize);
      if (ret > 0) {
         XrdSysCondVarHelper _lck(m_state_cond);
         m_delta_stats.AddBytesHit(ret);
         check_delta_stats();
      }
      return ret;
   }

   XrdOucIOVec readV( { iUserOff, iUserSize, 0, iUserBuff } );

   return ReadOpusCoalescere(io, &readV, 1, rh, "Read() ");
}

//------------------------------------------------------------------------------

int File::ReadV(IO *io, const XrdOucIOVec *readV, int readVnum, ReadReqRH *rh)
{
   TRACEF(Dump, "ReadV() for " << readVnum << " chunks.");

   m_state_cond.Lock();

   if (m_in_shutdown || io->m_in_detach)
   {
      m_state_cond.UnLock();
      return m_in_shutdown ? -ENOENT : -EBADF;
   }

   // Shortcut -- file is fully downloaded.

   if (m_cfi.IsComplete())
   {
      m_state_cond.UnLock();
      int ret = m_data_file->ReadV(const_cast<XrdOucIOVec*>(readV), readVnum);
      if (ret > 0) {
         XrdSysCondVarHelper _lck(m_state_cond);
         m_delta_stats.AddBytesHit(ret);
         check_delta_stats();
      }
      return ret;
   }

   return ReadOpusCoalescere(io, readV, readVnum, rh, "ReadV() ");
}

//------------------------------------------------------------------------------

int File::ReadOpusCoalescere(IO *io, const XrdOucIOVec *readV, int readVnum,
                             ReadReqRH *rh, const char *tpfx)
{
   // Non-trivial processing for Read and ReadV.
   // Entered under lock.
   //
   // loop over reqired blocks:
   //   - if on disk, ok;
   //   - if in ram or incoming, inc ref-count
   //   - otherwise request and inc ref count (unless RAM full => request direct)
   // unlock

   int prefetch_cnt = 0;

   ReadRequest    *read_req = nullptr;
   BlockRunList_t  runs_to_request;  // runs we are issuing a new remote request for

   std::unordered_map<Block*, std::vector<ChunkRequest>> blks_ready;

   // Blocks that are neither in RAM nor on disk, keyed by block index. Sorted
   // order is what run grouping needs, and the map folds together the several
   // chunks of one readV that can land in the same block.
   struct PendingChunk { char *m_buf; long long m_blk_off; int m_size; };
   std::map<int, std::vector<PendingChunk>> to_fetch;

   std::vector<XrdOucIOVec> iovec_disk;
   std::vector<XrdOucIOVec> iovec_direct;
   int                      iovec_disk_total = 0;
   int                      iovec_direct_total = 0;

   for (int iov_idx = 0; iov_idx < readVnum; ++iov_idx)
   {
      const XrdOucIOVec &iov = readV[iov_idx];
      long long   iUserOff  = iov.offset;
      int         iUserSize = iov.size;
      char       *iUserBuff = iov.data;

      const int idx_first = iUserOff / m_block_size;
      const int idx_last  = (iUserOff + iUserSize - 1) / m_block_size;

      TRACEF(DumpXL, tpfx << "sid: " << Xrd::hex1 << rh->m_seq_id << " idx_first: " << idx_first << " idx_last: " << idx_last);

      enum LastBlock_e { LB_other, LB_disk };

      LastBlock_e lbe = LB_other;

      for (int block_idx = idx_first; block_idx <= idx_last; ++block_idx)
      {
         TRACEF(DumpXL, tpfx << "sid: " << Xrd::hex1 << rh->m_seq_id << " idx: " << block_idx);
         BlockMap_i bi = m_block_map.find(block_idx);

         // overlap and read
         long long off = 0;     // offset in user buffer
         long long blk_off = 0; // offset in block
         int       size = 0;    // size to copy

         overlap(block_idx, m_block_size, iUserOff, iUserSize, off, blk_off, size);

         // In RAM or incoming?
         if (bi != m_block_map.end())
         {
            inc_ref_count(bi->second);
            TRACEF(Dump, tpfx << (void*) iUserBuff << " inc_ref_count for existing block " << bi->second << " idx = " <<  block_idx);

            if (bi->second->is_finished())
            {
               // note, blocks with error should not be here !!!
               // they should be either removed or reissued in ProcessRunResponse()
               assert(bi->second->is_ok());

               blks_ready[bi->second].emplace_back( ChunkRequest(nullptr, iUserBuff + off, blk_off, size) );

               if (bi->second->m_prefetch)
                  ++prefetch_cnt;
            }
            else
            {
               if ( ! read_req)
                  read_req = new ReadRequest(io, rh);

               // We have a lock on state_cond --> as we register the request before releasing the lock,
               // we are sure to get a call-in via the ChunkRequest handling when this block arrives.

               bi->second->m_chunk_reqs.emplace_back( ChunkRequest(read_req, iUserBuff + off, blk_off, size) );
               ++read_req->m_n_chunk_reqs;
            }

            lbe = LB_other;
         }
         // On disk?
         else if (m_cfi.TestBitWritten(offsetIdx(block_idx)))
         {
            TRACEF(DumpXL, tpfx << "read from disk " <<  (void*)iUserBuff << " idx = " << block_idx);

            if (lbe == LB_disk)
               iovec_disk.back().size += size;
            else
               iovec_disk.push_back( { block_idx * m_block_size + blk_off, size, 0, iUserBuff + off } );
            iovec_disk_total += size;

            if (m_cfi.TestBitPrefetch(offsetIdx(block_idx)))
               ++prefetch_cnt;

            lbe = LB_disk;
         }
         // Neither ... then we have to go get it. Record it and decide on run
         // grouping once the whole request has been classified -- a run needs
         // its length up front, as it is one RAM allocation.
         else
         {
            if ( ! read_req)
               read_req = new ReadRequest(io, rh);

            to_fetch[block_idx].push_back( { iUserBuff + off, blk_off, size } );

            lbe = LB_other;
         }
      } // end for over blocks in an IOVec
   } // end for over readV IOVec

   // Group the blocks we have to fetch into runs of consecutive indices, one
   // remote request and one disk write each. Runs are capped at
   // m_max_run_blocks; when the block size is at or above the target IO size
   // that cap is one block and every run holds exactly one.
   for (auto it = to_fetch.begin(); it != to_fetch.end(); )
   {
      const int first = it->first;

      int  n  = 1;
      auto jt = std::next(it);
      while (jt != to_fetch.end() && jt->first == first + n && n < m_max_run_blocks)
      {
         ++n; ++jt;
      }

      BlockRun *run = PrepareBlockRun(first, n, io, read_req, false);

      // Out of RAM for a whole run -- a single block may still fit.
      if ( ! run && n > 1)
      {
         n   = 1;
         jt  = std::next(it);
         run = PrepareBlockRun(first, 1, io, read_req, false);
      }

      if (run)
      {
         const int n_got = run->get_n_blocks();

         for (int k = 0; k < n_got; ++k)
         {
            Block *b = run->m_blocks[k];
            for (const PendingChunk &pc : to_fetch[first + k])
            {
               TRACEF(Dump, tpfx << "inc_ref_count new " << (void*)pc.m_buf << " idx = " << (first + k));
               inc_ref_count(b);
               b->m_chunk_reqs.emplace_back(ChunkRequest(read_req, pc.m_buf, pc.m_blk_off, pc.m_size));
               ++read_req->m_n_chunk_reqs;
            }
         }

         TRACEF(DumpXL, tpfx << "new run idx " << first << " n_blocks " << n_got);

         runs_to_request.push_back(run);

         it = to_fetch.lower_bound(first + n_got);
      }
      else
      {
         // Nope ... read these directly without caching.
         for (auto kt = it; kt != jt; ++kt)
         {
            for (const PendingChunk &pc : kt->second)
            {
               TRACEF(DumpXL, tpfx << "direct block " << kt->first << ", blk_off " << pc.m_blk_off <<
                      ", size " << pc.m_size);

               read_req->m_direct_done = false;
               iovec_direct_total += pc.m_size;

               long long in_offset = (long long) kt->first * m_block_size + pc.m_blk_off;
               char     *out_pos   = pc.m_buf;
               int       size      = pc.m_size;

               // Coalesce only when both the file extent and the destination
               // buffer continue where the previous entry ended -- chunks of a
               // readV are adjacent in the file far more often than in the
               // caller's buffers.
               if ( ! iovec_direct.empty())
               {
                  XrdOucIOVec &bk = iovec_direct.back();
                  if (bk.offset + bk.size == in_offset &&
                      bk.data   + bk.size == out_pos   &&
                      bk.size   + size    <= XrdProto::maxRVdsz)
                  {
                     bk.size += size;
                     continue;
                  }
               }

               // Make sure we do not issue a ReadV with a chunk size above
               // XrdProto::maxRVdsz. The number of actual ReadVs issued so as to
               // not exceed the XrdProto::maxRvecsz limit is determined in
               // RequestBlocksDirect().
               while (size > XrdProto::maxRVdsz)
               {
                  iovec_direct.push_back( { in_offset, XrdProto::maxRVdsz, 0, out_pos } );
                  in_offset += XrdProto::maxRVdsz;
                  out_pos   += XrdProto::maxRVdsz;
                  size      -= XrdProto::maxRVdsz;
               }
               iovec_direct.push_back( { in_offset, size, 0, out_pos } );
            }
         }

         it = jt;
      }
   }

   inc_prefetch_hit_cnt(prefetch_cnt);

   m_state_cond.UnLock();

   // First, send out remote requests for new blocks.
   if ( ! runs_to_request.empty())
   {
      ProcessRunRequests(runs_to_request, io);
      runs_to_request.clear();
   }

   // Second, send out remote direct read requests.
   if ( ! iovec_direct.empty())
   {
      RequestBlocksDirect(io, read_req, iovec_direct, iovec_direct_total);

      TRACEF(Dump, tpfx << "direct read requests sent out, n_chunks = " << (int) iovec_direct.size() << ", total_size = " << iovec_direct_total);
   }

   // Begin synchronous part where we process data that is already in RAM or on disk.

   long long bytes_read = 0;
   int       error_cond = 0; // to be set to -errno

   // Third, process blocks that are available in RAM.
   if ( ! blks_ready.empty())
   {
      for (auto &bvi : blks_ready)
      {
         for (auto &cr : bvi.second)
         {
            TRACEF(DumpXL, tpfx << "ub=" << (void*)cr.m_buf << " from pre-finished block " << bvi.first->m_offset/m_block_size << " size " << cr.m_size);
            memcpy(cr.m_buf, bvi.first->m_buff + cr.m_off, cr.m_size);
            bytes_read += cr.m_size;
         }
      }
   }

   // Fourth, read blocks from disk.
   if ( ! iovec_disk.empty())
   {
      int rc = ReadBlocksFromDisk(iovec_disk, iovec_disk_total);
      TRACEF(DumpXL, tpfx << "from disk finished size = " << rc);
      if (rc >= 0)
      {
         bytes_read += rc;
      }
      else
      {
         error_cond = rc;
         TRACEF(Error, tpfx << "failed read from disk");
      }
   }

   // End synchronous part -- update with sync stats and determine actual state of this read.
   // Note: remote reads might have already finished during disk-read!

   m_state_cond.Lock();

   for (auto &bvi : blks_ready)
      dec_ref_count(bvi.first, (int) bvi.second.size());

   if (read_req)
   {
      read_req->m_bytes_read += bytes_read;
      if (error_cond)
         read_req->update_error_cond(error_cond);
      read_req->m_stats.m_BytesHit += bytes_read;
      read_req->m_sync_done = true;

      if (read_req->is_complete())
      {
         // Almost like FinalizeReadRequest(read_req) -- but no callout!
         m_delta_stats.AddReadStats(read_req->m_stats);
         check_delta_stats();
         m_state_cond.UnLock();

         int ret = read_req->return_value();
         delete read_req;
         return ret;
      }
      else
      {
         m_state_cond.UnLock();
         return -EWOULDBLOCK;
      }
   }
   else
   {
      m_delta_stats.m_BytesHit += bytes_read;
      check_delta_stats();
      m_state_cond.UnLock();

      // !!! No callout.

      return error_cond ? error_cond : bytes_read;
   }
}


//==============================================================================
// WriteBlock and Sync
//==============================================================================

void File::WriteRunToDisk(BlockRun* run)
{
   // Write the whole run out in a single write; a run is index-consecutive and
   // its blocks share one buffer, so this is one contiguous extent.

   const long long offset = run->m_offset - m_offset;
   const long long size   = run->get_data_size();
   ssize_t         retval;

   // Snapshot the block list: completing the last block of the run destroys it.
   std::vector<Block*> blks(run->m_blocks);
   const bool cksum_net_no_cksums = run->req_cksum_net() && ! run->has_cksums();

   if (m_cfi.IsCkSumCache())
      if (run->has_cksums())
         retval = m_data_file->pgWrite(run->get_buff(), offset, size, run->ref_cksum_vec().data(), 0);
      else
         retval = m_data_file->pgWrite(run->get_buff(), offset, size, 0, 0);
   else
      retval = m_data_file->Write(run->get_buff(), offset, size);

   if (retval < size)
   {
      if (retval < 0) {
         TRACEF(Error, "WriteToDisk() write error " << retval);
      } else {
         TRACEF(Error, "WriteToDisk() incomplete run write ret=" << retval << " (should be " << size << ")");
      }

      XrdSysCondVarHelper _lck(m_state_cond);

      for (Block *b : blks)
         dec_ref_count(b);

      return;
   }

   TRACEF(Dump, "WriteToDisk() success set bits for run at " << run->m_offset <<
          " n_blocks=" << (int) blks.size() << " size=" << size);

   bool schedule_sync = false;
   {
      XrdSysCondVarHelper _lck(m_state_cond);

      if (cksum_net_no_cksums && m_cfi.IsCkSumNet())
      {
         m_cfi.ResetCkSumNet();
      }

      for (Block *b : blks)
      {
         const int blk_idx = (b->m_offset - m_offset) / m_block_size;

         m_cfi.SetBitWritten(blk_idx);

         if (b->m_prefetch)
         {
            m_cfi.SetBitPrefetch(blk_idx);
         }

         // Set synced bit or stash block index if in actual sync.
         // Synced state is only written out to cinfo file when data file is synced.
         if (m_in_sync)
         {
            m_writes_during_sync.push_back(blk_idx);
            m_bytes_during_sync += b->m_size;
         }
         else
         {
            m_cfi.SetBitSynced(blk_idx);
            m_non_flushed_bytes += b->m_size;
         }
      }

      if ( ! m_in_sync &&
           (m_cfi.IsComplete() || m_non_flushed_bytes >= Cache::GetInstance().RefConfiguration().m_flushBytes) &&
           ! m_in_shutdown)
      {
         schedule_sync       = true;
         m_in_sync           = true;
         m_non_flushed_bytes = 0;
      }

      // As soon as the reference count is decreased on the blocks, the
      // file object may be deleted.  Thus, to avoid holding both locks at a time,
      // we defer the ref count decrease until later if a sync is needed
      if (!schedule_sync) {
         for (Block *b : blks)
            dec_ref_count(b);
      }
   }

   if (schedule_sync)
   {
      cache()->ScheduleFileSync(this);
      XrdSysCondVarHelper _lck(m_state_cond);
      for (Block *b : blks)
         dec_ref_count(b);
   }
}

//------------------------------------------------------------------------------

void File::Sync()
{
   TRACEF(Dump, "Sync()");

   int ret     = m_data_file->Fsync();
   bool errorp = false;
   if (ret == XrdOssOK)
   {
      Stats loc_stats;
      {
         XrdSysCondVarHelper _lck(&m_state_cond);
         report_and_merge_delta_stats();
         loc_stats = m_stats;
      }
      m_cfi.WriteIOStat(loc_stats);
      m_cfi.Write(m_info_file, m_filename.c_str());
      int cret = m_info_file->Fsync();
      if (cret != XrdOssOK)
      {
         TRACEF(Error, "Sync cinfo file sync error " << cret);
         errorp = true;
      }
   }
   else
   {
      TRACEF(Error, "Sync data file sync error " << ret << ", cinfo file has not been updated");
      errorp = true;
   }

   if (errorp)
   {
      TRACEF(Error, "Sync failed, unlinking local files and initiating shutdown of File object");

      // Unlink will also call this->initiate_emergency_shutdown()
      Cache::GetInstance().UnlinkFile(m_filename, false);

      XrdSysCondVarHelper _lck(&m_state_cond);

      m_writes_during_sync.clear();
      m_in_sync = false;

      return;
   }

   int  written_while_in_sync;
   bool resync = false;
   {
      XrdSysCondVarHelper _lck(&m_state_cond);
      for (std::vector<int>::iterator i = m_writes_during_sync.begin(); i != m_writes_during_sync.end(); ++i)
      {
         m_cfi.SetBitSynced(*i);
      }
      written_while_in_sync = (int) m_writes_during_sync.size();
      m_non_flushed_bytes   = m_bytes_during_sync;
      m_writes_during_sync.clear();
      m_bytes_during_sync = 0;

      // If there were writes during sync and the file is now complete,
      // let us call Sync again without resetting the m_in_sync flag.
      if (written_while_in_sync > 0 && m_cfi.IsComplete() && ! m_in_shutdown)
         resync = true;
      else
         m_in_sync = false;
   }
   TRACEF(Dump, "Sync "<< written_while_in_sync  << " blocks written during sync." << (resync ? " File is now complete - resyncing." : ""));

   if (resync)
      Sync();
}


//==============================================================================
// Block processing
//==============================================================================

void File::free_block(Block* b)
{
   // Method always called under lock.
   // The RAM buffer belongs to the run, so it is released -- and the run
   // deleted -- only when the last block of the run goes away.
   int i = b->m_offset / m_block_size;
   TRACEF(Dump, "free_block block " << b << "  idx =  " <<  i);
   size_t ret = m_block_map.erase(i);
   if (ret != 1)
   {
      // assert might be a better option than a warning
      TRACEF(Error, "free_block did not erase " <<  i  << " from map");
   }
   else
   {
      BlockRun *run = b->m_run;
      delete b;

      if (--run->m_n_live == 0)
      {
         cache()->ReleaseRAM(run->m_buff, run->m_alloc_size);
         delete run;
      }
   }

   if (m_prefetch_state == kHold && (int) m_block_map.size() < m_prefetch_max_blocks_in_flight)
   {
      m_prefetch_state = kOn;
      cache()->RegisterPrefetchFile(this);
   }
}

//------------------------------------------------------------------------------

void File::release_run_ref(BlockRun* run)
{
   // Method always called under lock, and only once the run's blocks are all
   // finished (downloaded or in error), as dec_ref_count asserts on that.
   // The snapshot is required: the last dec_ref_count deletes the run.

   std::vector<Block*> blks(run->m_blocks);

   for (Block *b : blks)
      dec_ref_count(b);
}

//------------------------------------------------------------------------------

bool File::select_current_io_or_disable_prefetching(bool skip_current)
{
   // Method always called under lock. It also expects prefetch to be active.

   int  io_size = (int) m_io_set.size();
   bool io_ok   = false;

   if (io_size == 1)
   {
      io_ok = (*m_io_set.begin())->m_allow_prefetching;
      if (io_ok)
      {
         m_current_io = m_io_set.begin();
      }
   }
   else if (io_size > 1)
   {
      IoSet_i mi = m_current_io;
      if (skip_current && mi != m_io_set.end()) ++mi;

      for (int i = 0; i < io_size; ++i)
      {
         if (mi == m_io_set.end()) mi = m_io_set.begin();

         if ((*mi)->m_allow_prefetching)
         {
            m_current_io = mi;
            io_ok = true;
            break;
         }
         ++mi;
      }
   }

   if ( ! io_ok)
   {
      m_current_io    = m_io_set.end();
      m_prefetch_state = kStopped;
      cache()->DeRegisterPrefetchFile(this);
   }

   return io_ok;
}

//------------------------------------------------------------------------------

void File::ProcessDirectReadFinished(ReadRequest *rreq, int bytes_read, int error_cond)
{
   // Called from DirectResponseHandler.
   // NOT under lock.

   if (error_cond)
      TRACEF(Error, "Read(), direct read finished with error " << -error_cond << " " << XrdSysE2T(-error_cond));

   m_state_cond.Lock();

   if (error_cond)
      rreq->update_error_cond(error_cond);
   else {
      rreq->m_stats.m_BytesBypassed += bytes_read;
      rreq->m_bytes_read += bytes_read;
   }

   rreq->m_direct_done = true;

   bool rreq_complete = rreq->is_complete();

   m_state_cond.UnLock();

   if (rreq_complete)
      FinalizeReadRequest(rreq);
}

void File::ProcessBlockError(Block *b, ReadRequest *rreq)
{
   // Called from ProcessRunResponse().
   // YES under lock -- we have to protect m_block_map for recovery through multiple IOs.
   // Does not manage m_read_req.
   // Will not complete the request.

   TRACEF(Debug, "ProcessBlockError() io " << b->get_io() << ", block "<< b->m_offset/m_block_size <<
                 " finished with error " << -b->get_error() << " " << XrdSysE2T(-b->get_error()));

   rreq->update_error_cond(b->get_error());
   --rreq->m_n_chunk_reqs;

   dec_ref_count(b);
}

void File::ProcessBlockSuccess(Block *b, ChunkRequest &creq)
{
   // Called from ProcessRunResponse().
   // NOT under lock as it does memcopy ofor exisf block data.
   // Acquires lock for block, m_read_req and rreq state update.

   ReadRequest *rreq = creq.m_read_req;

   TRACEF(Dump, "ProcessBlockSuccess() ub=" << (void*)creq.m_buf  << " from finished block " << b->m_offset/m_block_size << " size " << creq.m_size);
   memcpy(creq.m_buf, b->m_buff + creq.m_off, creq.m_size);

   m_state_cond.Lock();

   rreq->m_bytes_read += creq.m_size;

   if (b->get_req_id() == (void*) rreq)
      rreq->m_stats.m_BytesMissed += creq.m_size;
   else
      rreq->m_stats.m_BytesHit    += creq.m_size;

   --rreq->m_n_chunk_reqs;

   if (b->m_prefetch)
      inc_prefetch_hit_cnt(1);

   dec_ref_count(b);

   bool rreq_complete = rreq->is_complete();

   m_state_cond.UnLock();

   if (rreq_complete)
      FinalizeReadRequest(rreq);
}

void File::FinalizeReadRequest(ReadRequest *rreq)
{
   // called from ProcessRunResponse()
   // NOT under lock -- does callout
   {
      XrdSysCondVarHelper _lck(m_state_cond);
      m_delta_stats.AddReadStats(rreq->m_stats);
      check_delta_stats();
   }

   rreq->m_rh->Done(rreq->return_value());
   delete rreq;
}

void File::ProcessRunResponse(BlockRun *run, int res)
{
   static const char* tpfx = "ProcessRunResponse ";

   const int  data_size = run->get_data_size();
   IO        *run_io    = run->get_io();

   TRACEF(Dump, tpfx << "run=" << run << ", idx=" << run->m_first_idx <<
          ", n_blocks=" << run->get_n_blocks() << ", off=" << run->m_offset <<
          ", size=" << data_size << ", res=" << res);

   if (res >= 0 && res != data_size)
   {
      // Incorrect number of bytes received, apparently size of the file on the remote
      // is different than what the cache expects it to be.
      TRACEF(Error, tpfx << "Wrong number of bytes received, assuming remote/local file size mismatch, unlinking local files and initiating shutdown of File object");
      Cache::GetInstance().UnlinkFile(m_filename, false);
   }

   m_state_cond.Lock();

   // run->m_blocks stays valid for the whole of this function: PrepareBlockRun
   // holds a reference on every block on the run's behalf, released below in
   // release_run_ref() -- or carried over to the next attempt on a reissue.

   // Deregister run from IO's prefetch count, if needed.
   if (run->m_prefetch_run)
   {
      IoSet_i mi = m_io_set.find(run_io);
      if (mi != m_io_set.end())
      {
         --run_io->m_active_prefetches;

         // If failed and IO is still prefetching -- disable prefetching on this IO.
         if (res < 0 && run_io->m_allow_prefetching)
         {
            TRACEF(Debug, tpfx << "after failed prefetch on io " << run_io << " disabling prefetching on this io.");
            run_io->m_allow_prefetching = false;

            // Check if any IO is still available for prfetching. If not, stop it.
            if (m_prefetch_state == kOn || m_prefetch_state == kHold)
            {
               if ( ! select_current_io_or_disable_prefetching(false) )
               {
                  TRACEF(Debug, tpfx << "stopping prefetching after io " << run_io << " marked as bad.");
               }
            }
         }

         if (res == data_size)
            m_prefetch_bytes += data_size;
      }
      else
      {
         TRACEF(Error, tpfx << "io " << run_io << " not found in IoSet.");
      }
   }

   if (res == data_size)
   {
      for (Block *b : run->m_blocks)
         b->set_downloaded();

      TRACEF(Dump, tpfx << "inc_ref_count idx=" << run->m_first_idx << " n=" << run->get_n_blocks());

      if ( ! m_in_shutdown)
      {
         // Increase ref-count for the writer, one per block. The run is the
         // write-queue entry and is written out as a single extent.
         for (Block *b : run->m_blocks)
            inc_ref_count(b);
         m_delta_stats.AddWriteStats(data_size, run->get_n_cksum_errors());
         // No check for writes, report-and-merge forced during Sync().
         cache()->AddWriteTask(run, true);
      }

      // Swap chunk-reqs vectors out of the Blocks, they are processed outside of lock.
      std::vector<std::pair<Block*, vChunkRequest_t>> creqs_to_notify;
      for (Block *b : run->m_blocks)
      {
         if ( ! b->m_chunk_reqs.empty())
         {
            creqs_to_notify.emplace_back(b, vChunkRequest_t());
            creqs_to_notify.back().second.swap(b->m_chunk_reqs);
         }
      }

      release_run_ref(run);

      m_state_cond.UnLock();

      for (auto &bcr : creqs_to_notify)
      {
         for (ChunkRequest &creq : bcr.second)
            ProcessBlockSuccess(bcr.first, creq);
      }
   }
   else
   {
      if (res < 0) {
         bool new_error = run_io->register_block_error(res);
         int tlvl = new_error ? TRACE_Error : TRACE_Debug;
         TRACEF_INT(tlvl, tpfx << "run " << run << ", idx=" << run->m_first_idx << ", off=" << run->m_offset
                    << ", io=" << run_io << ", error=" << res);
      } else {
         bool first_p = run_io->register_incomplete_read();
         int tlvl = first_p ? TRACE_Error : TRACE_Debug;
         TRACEF_INT(tlvl, tpfx << "run " << run << ", idx=" << run->m_first_idx << ", off=" << run->m_offset
                    << ", io=" << run_io << " incomplete, got " << res << " expected " << data_size);
#if defined(__APPLE__) || defined(__GNU__) || (defined(__FreeBSD_kernel__) && defined(__GLIBC__)) || defined(__FreeBSD__)
         res = -EIO;
#else
         res = -EREMOTEIO;
#endif
      }

      for (Block *b : run->m_blocks)
         b->set_error(res);

      // Loop over the Blocks' chunk-req vectors, error out ones with the same IO.
      // Collect others with a different IO, the first of them will be used to
      // reissue the request. This is then done outside of lock.
      std::list<ReadRequest*> rreqs_to_complete;
      ReadRequest            *reissue_rreq = nullptr;

      for (Block *b : run->m_blocks)
      {
         vChunkRequest_t creqs_to_keep;

         for (ChunkRequest &creq : b->m_chunk_reqs)
         {
            ReadRequest *rreq = creq.m_read_req;

            if (rreq->m_io == run_io)
            {
               ProcessBlockError(b, rreq);
               if (rreq->is_complete())
               {
                  rreqs_to_complete.push_back(rreq);
               }
            }
            else
            {
               creqs_to_keep.push_back(creq);
               if ( ! reissue_rreq) reissue_rreq = rreq;
            }
         }

         b->m_chunk_reqs.swap(creqs_to_keep);
      }

      bool reissue = false;
      if (reissue_rreq)
      {
         TRACEF(Debug, tpfx << "requested run " << (void*)run << " failed with another io " <<
                run_io << " - reissuing request with my io " << reissue_rreq->m_io);

         // Keeps the run's block references for the next attempt.
         run->reset_error_and_set_io(reissue_rreq->m_io, reissue_rreq);
         reissue = true;
      }
      else
      {
         release_run_ref(run);
      }

      m_state_cond.UnLock();

      for (auto rreq : rreqs_to_complete)
         FinalizeReadRequest(rreq);

      if (reissue)
         ProcessRunRequest(run);
   }
}

//------------------------------------------------------------------------------

void File::ProcessSequenceResponse(BlockSequenceResponseHandler *h, int res)
{
   // A batched ReadV yields a single result for all of its runs, so they
   // succeed or fail together.

   if (res == h->m_expected_size)
   {
      for (BlockRun *run : h->m_runs)
         ProcessRunResponse(run, run->get_data_size());
      return;
   }

   if (res >= 0)
   {
      TRACEF(Error, "ProcessSequenceResponse() short ReadV over " << (int) h->m_runs.size() <<
             " runs, got " << res << " expected " << h->m_expected_size <<
             ", assuming remote/local file size mismatch, unlinking local files and initiating shutdown of File object");
      Cache::GetInstance().UnlinkFile(m_filename, false);
#if defined(__APPLE__) || defined(__GNU__) || (defined(__FreeBSD_kernel__) && defined(__GLIBC__)) || defined(__FreeBSD__)
      res = -EIO;
#else
      res = -EREMOTEIO;
#endif
   }

   // Hand each run the failure; the per-run error path takes it from here.
   for (BlockRun *run : h->m_runs)
      ProcessRunResponse(run, res);
}

//------------------------------------------------------------------------------

const char* File::lPath() const
{
   return m_filename.c_str();
}

//------------------------------------------------------------------------------

int File::offsetIdx(int iIdx) const
{
   return iIdx - m_offset/m_block_size;
}


//------------------------------------------------------------------------------

void File::Prefetch()
{
   // Check that block is not on disk and not in RAM.
   // TODO: Could prefetch several blocks at once!
   //       blks_max could be an argument

   BlockRunList_t runs;
   IO            *prefetch_io = nullptr;

   TRACEF(DumpXL, "Prefetch() entering.");
   {
      XrdSysCondVarHelper _lck(m_state_cond);

      if (m_prefetch_state != kOn)
      {
         return;
      }

      if ( ! select_current_io_or_disable_prefetching(true) )
      {
         TRACEF(Error, "Prefetch no available IO object found, prefetching stopped. This should not happen, i.e., prefetching should be stopped before.");
         return;
      }

      // Select block(s) to fetch.
      for (int f = 0; f < m_num_blocks; ++f)
      {
         if ( ! m_cfi.TestBitWritten(f))
         {
            int f_act = f + m_offset / m_block_size;

            BlockMap_i bi = m_block_map.find(f_act);
            if (bi == m_block_map.end())
            {
               BlockRun *run = PrepareBlockRun(f_act, 1, *m_current_io, nullptr, true);
               if (run)
               {
                  TRACEF(Dump, "Prefetch take block " << f_act);
                  runs.push_back(run);
                  // Note: only the run's own ref is held; the writer ref is taken
                  // when the run is placed into the write queue.

                  inc_prefetch_read_cnt(1);
               }
               else
               {
                  // This shouldn't happen as prefetching stops when RAM is 70% full.
                  TRACEF(Warning, "Prefetch allocation failed for block " << f_act);
               }
               break;
            }
         }
      }

      if (runs.empty())
      {
         TRACEF(Debug, "Prefetch file is complete, stopping prefetch.");
         m_prefetch_state = kComplete;
         cache()->DeRegisterPrefetchFile(this);
      }
      else
      {
         (*m_current_io)->m_active_prefetches += (int) runs.size();
         prefetch_io = *m_current_io;
      }
   }

   if ( ! runs.empty())
   {
      ProcessRunRequests(runs, prefetch_io);
   }
}


//------------------------------------------------------------------------------

float File::GetPrefetchScore() const
{
   return m_prefetch_score;
}

XrdSysError* File::GetLog() const
{
   return Cache::TheOne().GetLog();
}

XrdSysTrace* File::GetTrace() const
{
   return Cache::TheOne().GetTrace();
}

void File::insert_remote_location(const std::string &loc)
{
   if ( ! loc.empty())
   {
      size_t p = loc.find_first_of('@');
      m_remote_locations.insert(&loc[p != std::string::npos ? p + 1 : 0]);
   }
}

std::string File::GetRemoteLocations() const
{
   std::string s;
   if ( ! m_remote_locations.empty())
   {
      size_t      sl = 0;
      int         nl = 0;
      for (std::set<std::string>::iterator i = m_remote_locations.begin(); i != m_remote_locations.end(); ++i, ++nl)
      {
         sl += i->size();
      }
      s.reserve(2 + sl + 2*nl + nl - 1 + 1);
      s = '[';
      int j = 1;
      for (std::set<std::string>::iterator i = m_remote_locations.begin(); i != m_remote_locations.end(); ++i, ++j)
      {
         s += '"'; s += *i; s += '"';
         if (j < nl) s += ',';
      }
      s += ']';
   }
   else
   {
      s = "[]";
   }
   return s;
}

//==============================================================================
//=======================    RESPONSE HANDLERS    ==============================
//==============================================================================

void BlockRunResponseHandler::Done(int res)
{
   m_run->m_file->ProcessRunResponse(m_run, res);
   delete this;
}

//------------------------------------------------------------------------------

void BlockSequenceResponseHandler::Done(int res)
{
   m_file->ProcessSequenceResponse(this, res);
   delete this;
}

//------------------------------------------------------------------------------

void DirectResponseHandler::Done(int res)
{
   m_mutex.Lock();

   int n_left = --m_to_wait;

   if (res < 0) {
      if (m_errno == 0) m_errno = res; // store first reported error
   } else {
      m_bytes_read += res;
   }

   m_mutex.UnLock();

   if (n_left == 0)
   {
      m_file->ProcessDirectReadFinished(m_read_req, m_bytes_read, m_errno);
      delete this;
   }
}
