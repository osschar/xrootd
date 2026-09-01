#ifndef __XRDPOSIXPREPIO_HH__
#define __XRDPOSIXPREPIO_HH__
/******************************************************************************/
/*                                                                            */
/*                     X r d P o s i x P r e p I O . h h                      */
/*                                                                            */
/* (c) 2016 by the Board of Trustees of the Leland Stanford, Jr., University  */
/*                            All Rights Reserved                             */
/*   Produced by Andrew Hanushevsky for Stanford University under contract    */
/*              DE-AC02-76-SFO0515 with the Department of Energy              */
/*                                                                            */
/* This file is part of the XRootD software suite.                            */
/*                                                                            */
/* XRootD is free software: you can redistribute it and/or modify it under    */
/* the terms of the GNU Lesser General Public License as published by the     */
/* Free Software Foundation, either version 3 of the License, or (at your     */
/* option) any later version.                                                 */
/*                                                                            */
/* XRootD is distributed in the hope that it will be useful, but WITHOUT      */
/* ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or      */
/* FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public       */
/* License for more details.                                                  */
/*                                                                            */
/* You should have received a copy of the GNU Lesser General Public License   */
/* along with XRootD in a file called COPYING.LESSER (LGPL license) and file  */
/* COPYING (GPL license).  If not, see <http://www.gnu.org/licenses/>.        */
/*                                                                            */
/* The copyright holder's institutional names and contributor's names may not */
/* be used to endorse or promote products derived from this software without  */
/* specific prior written permission of the institution or contributor.       */
/******************************************************************************/

#include "Xrd/XrdJob.hh"
#include "XrdPosix/XrdPosixFile.hh"

class XrdOucCacheIOCD;

class XrdPosixPrepIO : public XrdOucCacheIO
{
public:

bool        Detach(XrdOucCacheIOCD &cdP) {(void)cdP; return true;}

void        Disable();

int         Fcntl(XrdOucCacheOp::Code opc, const std::string& args,
                                                 std::string& resp)
                 {return (Init() ? fileP->Fcntl(opc, args, resp) : openRC);}

long long   FSize() {return (Init() ? fileP->FSize() : openRC);}

int         Fstat(struct stat &buf)
                 {return (Init() ? fileP->Fstat(buf) : openRC);}

int         Open() {Init(); return openRC;}

const char *Path()  {return fileP->Path();}

//! pgRead and pgReadV must be forwarded, not left to the XrdOucCacheIO
//! defaults. Those defaults fall back on Read(), which this class does
//! override, so the data would still be correct -- but the checksums would be
//! silently lost, and a caller that asked for end-to-end checksums has no way
//! to tell that from an origin which cannot supply them. XrdPfc, for one,
//! concludes the latter and permanently marks the file as having none.

int         pgRead(char *buff, long long offs, int rdlen,
                   std::vector<uint32_t> &csvec, uint64_t opts=0, int *csfix=0)
                  {return (Init() ? fileP->pgRead(buff, offs, rdlen, csvec,
                                                  opts, csfix) : openRC);}

void        pgRead(XrdOucCacheIOCB &iocb, char *buff, long long offs, int rdlen,
                   std::vector<uint32_t> &csvec, uint64_t opts=0, int *csfix=0)
                  {if (Init(&iocb))
                      fileP->pgRead(iocb, buff, offs, rdlen, csvec, opts, csfix);
                      else iocb.Done(openRC);
                  }

int         pgReadV(const XrdOucIOVec *readV, int rnum,
                    std::vector<uint32_t> *csvec, uint64_t opts=0, int *csfix=0)
                   {return (Init() ? fileP->pgReadV(readV, rnum, csvec,
                                                    opts, csfix) : openRC);}

void        pgReadV(XrdOucCacheIOCB &iocb, const XrdOucIOVec *readV, int rnum,
                    std::vector<uint32_t> *csvec, uint64_t opts=0, int *csfix=0)
                   {if (Init(&iocb))
                       fileP->pgReadV(iocb, readV, rnum, csvec, opts, csfix);
                       else iocb.Done(openRC);
                   }

int         Read (char *Buffer, long long Offset, int Length)
                 {return (Init() ? fileP->Read(Buffer, Offset, Length) : openRC);}

void        Read (XrdOucCacheIOCB &iocb, char *buff, long long offs, int rlen)
                 {if (Init(&iocb)) fileP->Read(iocb, buff, offs, rlen);
                     else iocb.Done(openRC);
                 }

int         ReadV(const XrdOucIOVec *readV, int n)
                 {return (Init() ? fileP->ReadV(readV, n) : openRC);}

void        ReadV(XrdOucCacheIOCB &iocb, const XrdOucIOVec *readV, int rnum)
                 {if (Init(&iocb)) fileP->ReadV(iocb, readV, rnum);
                     else iocb.Done(openRC);
                 }

int         Sync() {return (Init() ? fileP->Sync() : openRC);}

void        Sync(XrdOucCacheIOCB &iocb)
                 {if (Init(&iocb)) fileP->Sync(iocb);
                     else iocb.Done(openRC);
                 }

int         Trunc(long long Offset)
                 {return (Init() ? fileP->Trunc(Offset) : openRC);}

int         Write(char *Buffer, long long Offset, int Length)
                 {return (Init() ? fileP->Write(Buffer,Offset,Length) : openRC);}

void        Write(XrdOucCacheIOCB &iocb, char *buff, long long offs, int wlen)
                 {if (Init(&iocb)) fileP->Write(iocb, buff, offs, wlen);
                     else iocb.Done(openRC);
                 }

            XrdPosixPrepIO(XrdPosixFile *fP, XrdCl::OpenFlags::Flags clflags,
                            XrdCl::Access::Mode clmode)
                          : fileP(fP), openRC(0), iCalls(0),
                            clFlags(clflags), clMode(clmode) {}
virtual    ~XrdPosixPrepIO() {}

private:
bool          Init(XrdOucCacheIOCB *iocbP=0);

XrdPosixFile *fileP;
int           openRC;
int           iCalls;

XrdCl::OpenFlags::Flags clFlags;
XrdCl::Access::Mode     clMode;
};
#endif
