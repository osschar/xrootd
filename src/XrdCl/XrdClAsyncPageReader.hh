//------------------------------------------------------------------------------
// Copyright (c) 2011-2012 by European Organization for Nuclear Research (CERN)
// Author: Michal Simon <michal.simon@cern.ch>
//------------------------------------------------------------------------------
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
//------------------------------------------------------------------------------

#ifndef SRC_XRDCL_XRDCLASYNCPAGEREADER_HH_
#define SRC_XRDCL_XRDCLASYNCPAGEREADER_HH_

#include "XrdCl/XrdClXRootDResponses.hh"
#include "XrdCl/XrdClSocket.hh"
#include "XrdOuc/XrdOucPgrwUtils.hh"
#include "XrdSys/XrdSysPageSize.hh"

#include <sys/uio.h>
#include <memory>
#include <arpa/inet.h>

namespace XrdCl
{

//------------------------------------------------------------------------------
//! Object for reading out data from the PgRead response
//------------------------------------------------------------------------------
class AsyncPageReader
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param chunks  : list of buffer for the data
    //! @param digests : a vector that will be filled with crc32c digest data
    //--------------------------------------------------------------------------
    AsyncPageReader( ChunkList             &chunks,
                     std::vector<uint32_t> &digests ) :
        chunks( chunks ),
        digests( digests ),
        dlen( 0 ),
        rspoff( 0 ),
        chindex( 0 ),
        choff( 0 ),
        dgindex( 0 ),
        dgoff( 0 ),
        iovcnt( 0 ),
        iovindex( 0 )
    {
      //------------------------------------------------------------------------
      // Each chunk owns a range of the digest vector, since the chunks of a
      // pgreadv are separate extents of the file and each one is paged from
      // its own offset. For a pgread, which always has exactly one chunk, this
      // is the whole vector and comes to the same thing as before.
      //------------------------------------------------------------------------
      dgbase.reserve( chunks.size() + 1 );
      dgbase.push_back( 0 );
      for( auto &ch : chunks )
        dgbase.push_back( dgbase.back() +
                          ( ch.length ? XrdOucPgrwUtils::csNum( ch.offset,
                                                                ch.length ) : 0 ) );
      digests.resize( dgbase.back() );
      chbytes.resize( chunks.size(), 0 );
    }

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~AsyncPageReader()
    {
    }

    //--------------------------------------------------------------------------
    //! Sets message data size
    //--------------------------------------------------------------------------
    void SetRsp( ServerResponseV2 *rsp )
    {
      dlen = rsp->status.bdy.dlen;
      rspoff = rsp->info.pgread.offset;

      //------------------------------------------------------------------------
      // Find the extent this response belongs to. The chunks of a pgreadv are
      // disjoint and need not be in file order, so the only thing that locates
      // a response is which extent contains its offset -- walking the chunks
      // by cumulative length, as a contiguous read allows, does not work here.
      //------------------------------------------------------------------------
      for( chindex = 0; chindex < chunks.size(); ++chindex )
        if( rspoff >= chunks[chindex].offset &&
            rspoff <  chunks[chindex].offset + chunks[chindex].length )
          break;

      if( chindex >= chunks.size() )
      {
        //----------------------------------------------------------------------
        // No extent holds it. The server does this legitimately for the empty
        // final result that closes a read which returned nothing, in which
        // case dlen is zero and Read() will not touch anything. Anything else
        // is a bad response and is left to the message handler to notice.
        //----------------------------------------------------------------------
        choff   = 0;
        dgindex = dgbase.back();
        return;
      }

      uint64_t delta = rspoff - chunks[chindex].offset;
      choff   = delta;
      dgindex = dgbase[chindex] +
                ( delta ? XrdOucPgrwUtils::csNum( chunks[chindex].offset,
                                                  delta ) : 0 );
      dgoff   = 0;
    }

    //--------------------------------------------------------------------------
    //! @return : the first digest index of each chunk, plus the total as the
    //!           last entry, so entry i+1 is the end of chunk i's range
    //--------------------------------------------------------------------------
    const std::vector<size_t>& DigestBase() const { return dgbase; }

    //--------------------------------------------------------------------------
    //! @return : number of bytes placed into each chunk's buffer
    //--------------------------------------------------------------------------
    const std::vector<uint32_t>& ChunkBytes() const { return chbytes; }

    //--------------------------------------------------------------------------
    //! Readout data from the socket
    //! @param socket  : the socket with the data 
    //! @param btsread : number of user data read from the socket
    //! @return        : operation status
    //--------------------------------------------------------------------------
    XRootDStatus Read( Socket &socket, uint32_t &btsread )
    {
      if( dlen == 0 || chindex >= chunks.size() )
        return XRootDStatus();
      btsread = 0;
      int nbbts = 0;
      do
      {
        // Prepare the IO vector for receiving the data
        if( iov.empty() )
          InitIOV();
        // read the data into the buffer
        nbbts = 0;
        auto st = socket.ReadV( iov.data() + iovindex, iovcnt, nbbts );
        if( !st.IsOK() )
          return st;
        btsread += nbbts;
        dlen    -= nbbts;
        ShiftIOV( nbbts );
        if( st.code == suRetry )
          return st;
      }
      while( nbbts > 0 && dlen > 0 && chindex < chunks.size() );

      return XRootDStatus();
    }

  private:

    //--------------------------------------------------------------------------
    //! @return : maximum size of I/O vector
    //--------------------------------------------------------------------------
    inline static int max_iovcnt()
    {
      // make sure it is an even number
      static const int iovmax = XrdSys::getIovMax() & ~1;
      return iovmax;
    }

    //--------------------------------------------------------------------------
    //! Add I/O buffer to the vector
    //--------------------------------------------------------------------------
    inline void addiov( char *&buf, size_t len )
    {
      iov.emplace_back();
      iov.back().iov_base = buf;
      iov.back().iov_len  = len;
      buf += len;
      ++iovcnt;
    }

    //--------------------------------------------------------------------------
    //! Add I/O buffer to the vector and update number of bytes left to be read
    //--------------------------------------------------------------------------
    inline void addiov( char *&buf, uint32_t len, uint32_t &dleft )
    {
      if( len > dleft ) len = dleft;
      addiov( buf, len );
      dleft -= len;
    }

    //--------------------------------------------------------------------------
    //! Calculate the size of the I/O vector
    //! @param dleft : data to be accomodated by the I/O vector
    //--------------------------------------------------------------------------
    inline static uint32_t CalcIOVSize( uint32_t dleft )
    {
      uint32_t ret = ( dleft / PageWithDigest + 2 ) * 2;
      return ( ret > uint32_t( max_iovcnt() ) ? max_iovcnt() : ret );
    }

    //--------------------------------------------------------------------------
    //! Calculate the size of the data to be read
    //--------------------------------------------------------------------------
    uint32_t CalcRdSize()
    {
      // data size in the server response (including digests)
      uint32_t dleft = dlen;
      // space in our page buffer
      uint32_t pgspace = chunks[chindex].length - choff;
      // space in the current chunk's digest range, which a single response
      // never leaves, since a response carries data of one extent only
      size_t   dgend   = dgbase[chindex+1];
      uint32_t dgspace = ( dgend > dgindex
                         ? sizeof( uint32_t ) * ( dgend - dgindex ) - dgoff : 0 );
      if( dleft > pgspace + dgspace )
        dleft = pgspace + dgspace;
      return dleft;
    }

    //--------------------------------------------------------------------------
    //! Initialize the I/O vector
    //--------------------------------------------------------------------------
    void InitIOV()
    {
      iovindex = 0;
      // figure out the number of data we can read in one go
      uint32_t dleft = CalcRdSize();
      // and reset the I/O vector
      iov.clear();
      iovcnt = 0;
      iov.reserve( CalcIOVSize( dleft ) );
      // now prepare the page and digest buffers
      ChunkInfo ch    = chunks[chindex];
      char*     pgbuf = static_cast<char*>( ch.buffer ) + choff;
      uint64_t  rdoff = ch.offset + choff;
      char*     dgbuf = reinterpret_cast<char*>( digests.data() + dgindex ) + dgoff;
      // handle the first digest
      uint32_t fdglen = sizeof( uint32_t ) - dgoff;
      addiov( dgbuf, fdglen, dleft );
      if( dleft == 0 || iovcnt >= max_iovcnt() )
        return;
      // handle the first page
      uint32_t fpglen = XrdSys::PageSize - rdoff % XrdSys::PageSize;
      addiov( pgbuf, fpglen, dleft );
      if( dleft == 0 || iovcnt >= max_iovcnt() )
        return;
      // handle all the subsequent aligned pages
      size_t fullpgs = dleft / PageWithDigest;
      for( size_t i = 0; i < fullpgs; ++i )
      {
        addiov( dgbuf, sizeof( uint32_t ), dleft );
        if( dleft == 0 || iovcnt >= max_iovcnt() )
          return;
        addiov( pgbuf, XrdSys::PageSize, dleft );
        if( dleft == 0 || iovcnt >= max_iovcnt() )
          return;
      }
      // handle the last digest
      uint32_t ldglen = sizeof( uint32_t );
      addiov( dgbuf, ldglen, dleft );
      if( dleft == 0 || iovcnt >= max_iovcnt() )
        return;
      // handle the last page
      addiov( pgbuf, dleft, dleft );
    }

    //--------------------------------------------------------------------------
    //! Shift buffer by a number of bytes
    //--------------------------------------------------------------------------
    inline void shift( void *&buffer, size_t nbbts )
    {
      char *buf = static_cast<char*>( buffer );
      buf += nbbts;
      buffer = buf;
    }

    //--------------------------------------------------------------------------
    //! shift digest buffer by `btsread`
    //! @param btsread : total number of bytes read (will be decremented by bytes
    //!                  shifted in buffer)
    //--------------------------------------------------------------------------
    inline void shiftdgbuf( uint32_t &btsread )
    {
      if( iov[iovindex].iov_len > btsread )
      {
        iov[iovindex].iov_len -= btsread;
        shift( iov[iovindex].iov_base, btsread );
        dgoff += btsread;
        btsread = 0;
        return;
      }

      btsread -= iov[iovindex].iov_len;
      iov[iovindex].iov_len = 0;
      dgoff = 0;
      digests[dgindex] = ntohl( digests[dgindex] );
      ++dgindex;
      ++iovindex;
      --iovcnt;
    }

    //--------------------------------------------------------------------------
    //! shift page buffer by `btsread`
    //! @param btsread : total number of bytes read (will be decremented by bytes
    //!                  shifted in buffer)
    //--------------------------------------------------------------------------
    inline void shiftpgbuf( uint32_t &btsread )
    {
      if( iov[iovindex].iov_len > btsread )
      {
        iov[iovindex].iov_len -= btsread;
        shift( iov[iovindex].iov_base, btsread );
        choff += btsread;
        btsread = 0;
        return;
      }

      btsread -= iov[iovindex].iov_len;
      choff   += iov[iovindex].iov_len;
      iov[iovindex].iov_len = 0;
      ++iovindex;
      --iovcnt;
    }

    //--------------------------------------------------------------------------
    //! shift the I/O vector by the number of bytes read
    //--------------------------------------------------------------------------
    void ShiftIOV( uint32_t btsread )
    {
      // if iovindex is even it point to digest, otherwise it points to a page
      if( iovindex % 2 == 0 )
        shiftdgbuf( btsread );
      // adjust as many I/O buffers as necessary
      while( btsread > 0 )
      {
        // handle page
        shiftpgbuf( btsread );
        if( btsread == 0 ) break;
        // handle digest
        shiftdgbuf( btsread );
      }
      // if we filled the buffer, move to the next one
      if( iovcnt == 0 )
        iov.clear();
      // record how much of this chunk we have filled
      chbytes[chindex] = choff;
      // do we need to move to the next chunk?
      if( choff >= chunks[chindex].length )
      {
        ++chindex;
        choff = 0;
        if( chindex < chunks.size() ) dgindex = dgbase[chindex];
      }
    }

    ChunkList &chunks;              //< list of data chunks to be filled with user data
    std::vector<uint32_t> &digests; //< list of crc32c digests for every 4KB page of data
    std::vector<size_t>   dgbase;   //< first digest index of each chunk, plus the total
    std::vector<uint32_t> chbytes;  //< bytes placed into each chunk so far
    uint32_t   dlen;                //< size of the data in the message
    uint64_t   rspoff;              //< response offset

    size_t chindex;                 //< index of the current data buffer
    size_t choff;                   //< offset within the current buffer
    size_t dgindex;                 //< index of the current digest buffer
    size_t dgoff;                   //< offset within the current digest buffer

    std::vector<iovec> iov;         //< I/O vector
    int                iovcnt;      //< size of the I/O vector
    size_t             iovindex;    //< index of the first valid element in the I/O vector

    static const int PageWithDigest = XrdSys::PageSize + sizeof( uint32_t );
};

} /* namespace XrdEc */

#endif /* SRC_XRDCL_XRDCLASYNCPAGEREADER_HH_ */
