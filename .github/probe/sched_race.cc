// CI PROBE: does XrdScheduler lose a timer wakeup?
//
// XrdScheduler::TimeSched() computes how long to sleep while holding
// TimerMutex, releases it, and only then waits on TimerRings -- a condition
// variable with its own, different mutex. Schedule(jp, atime) inserts a job at
// the head of the timer queue under TimerMutex and calls TimerRings.Signal().
// A Signal() that lands after TimeSched() released TimerMutex but before it
// entered pthread_cond_timedwait() wakes nobody, and the timer thread then
// sleeps for the whole wait it computed -- up to an hour when the queue looked
// empty. Every timed job in the process stalls with it.
//
// This drives the real library through the pattern the g-stream auto-flush
// uses: a job whose DoIt() re-arms itself. Re-arming for time(0) rather than
// time(0)+1 makes the timer pop it immediately, so the race window comes round
// thousands of times a second instead of once. Background threads burning CPU
// make preemption inside the window likelier, as parallel ctest load does.
//
// Build: g++ -O2 -std=c++17 -I<src> sched_race.cc -L<lib> -lXrdUtils -pthread
// Usage: sched_race [seconds=60] [cpu_hogs=0] [stall_secs=3] [maxi=780]
// Reports every stall longer than stall_secs; exits 1 if there was any.

#include "Xrd/XrdJob.hh"
#include "Xrd/XrdScheduler.hh"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <thread>
#include <vector>

namespace
{
std::atomic<long> g_runs{0};
std::atomic<bool> g_stop{false};
XrdScheduler     *g_sched = nullptr;

class Rearm : public XrdJob
{
public:
   Rearm() : XrdJob(".rearm") {}   // leading '.' keeps it out of SCHED tracing

   void DoIt() override
   {
      g_runs.fetch_add(1, std::memory_order_relaxed);
      if (!g_stop.load()) g_sched->Schedule(this, time(0));
   }
};

double now()
{
   using namespace std::chrono;
   return duration<double>(steady_clock::now().time_since_epoch()).count();
}
}

int main(int argc, char *argv[])
{
   int secs  = argc > 1 ? atoi(argv[1]) : 60;
   int hogs  = argc > 2 ? atoi(argv[2]) : 0;
   int stall = argc > 3 ? atoi(argv[3]) : 3;

   std::vector<std::thread> burners;
   for (int i = 0; i < hogs; ++i)
      burners.emplace_back([] { volatile unsigned long x = 0; while (!g_stop.load()) ++x; });

   // maxi, the idle-worker trim interval, bounds how long a lost wakeup can
   // last. 780 s is the default of the constructor behind the server's global
   // Sched (Xrd/XrdGlobals.cc), unless "xrd.sched idle" changes it.
   int maxi = argc > 4 ? atoi(argv[4]) : 780;
   XrdScheduler sched(3, 128, maxi);
   g_sched = &sched;
   sched.Start();

   Rearm job;
   sched.Schedule(&job, time(0));

   // Record every gap longer than stall_secs and how long it lasted: a lost
   // wakeup leaves the timer thread asleep until the next timed event of any
   // kind -- the scheduler's own idle-worker trim, every maxi seconds, if
   // nothing else.
   double t0 = now(), t_last_change = t0;
   long   last = 0;
   int    n_stalls = 0;
   double worst = 0;
   bool   in_stall = false;
   while (now() - t0 < secs)
   {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      long r = g_runs.load();
      double t = now();
      if (r != last)
      {
         if (in_stall)
         {
            double gap = t - t_last_change;
            printf("STALL %d: no timed job ran for %.1f s, from %.1f s into the test, after %ld runs\n",
                   n_stalls, gap, t_last_change - t0, last);
            fflush(stdout);
            if (gap > worst) worst = gap;
            in_stall = false;
         }
         last = r; t_last_change = t;
         continue;
      }
      if (!in_stall && t - t_last_change > stall) { in_stall = true; ++n_stalls; }
   }
   if (in_stall)
   {
      double gap = now() - t_last_change;
      printf("STALL %d: still stalled at the end, %.1f s so far, after %ld runs\n", n_stalls, gap, last);
      if (gap > worst) worst = gap;
   }
   printf("%s: %d stalls longer than %d s in %d s, worst %.1f s; %ld runs\n",
          n_stalls ? "FAIL" : "ok", n_stalls, stall, secs, worst, last);
   int rc = n_stalls ? 1 : 0;

   fflush(stdout);
   // A stalled timer thread cannot be woken to shut down cleanly; just leave.
   _exit(rc);
}
