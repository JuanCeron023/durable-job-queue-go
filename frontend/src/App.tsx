import { useEffect, useState } from "react";
import type { JobState, Snapshot } from "./types";

const stateStyle: Record<JobState, string> = {
  pending: "bg-slate-600/40 text-slate-300",
  running: "bg-amber-500/20 text-amber-300",
  succeeded: "bg-emerald-500/20 text-emerald-300",
  failed: "bg-orange-500/20 text-orange-300",
  dead: "bg-rose-500/20 text-rose-300",
};

function Chip({ label, value, accent }: { label: string; value: React.ReactNode; accent?: string }) {
  return (
    <div className="bg-slate-900/60 border border-slate-800 rounded-xl p-3">
      <div className="text-[11px] uppercase tracking-wide text-slate-500">{label}</div>
      <div className={`text-xl font-semibold ${accent ?? ""}`}>{value}</div>
    </div>
  );
}

async function enqueue(body: unknown) {
  await fetch("/api/jobs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

function ticket() {
  return {
    type: "send_ticket_email",
    priority: 5,
    max_attempts: 5,
    payload: { to: `fan${Math.floor(Math.random() * 9999)}@example.com`, event_id: "venue-night-01" },
  };
}

export default function App() {
  const [snap, setSnap] = useState<Snapshot | null>(null);

  useEffect(() => {
    const refresh = async () => {
      try {
        setSnap(await fetch("/api/snapshot").then((r) => r.json() as Promise<Snapshot>));
      } catch {
        /* ignore */
      }
    };
    refresh();
    const id = setInterval(refresh, 700);
    return () => clearInterval(id);
  }, []);

  const c = snap?.queue.counts ?? {};

  return (
    <div className="max-w-6xl mx-auto px-6 py-8">
      <header className="flex items-center gap-3 mb-8">
        <div className="h-10 w-10 rounded-xl bg-gradient-to-br from-teal-300 to-emerald-500 grid place-items-center font-bold text-slate-900">
          TF
        </div>
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">TickFlow</h1>
          <p className="text-sm text-slate-300/70">
            Durable background jobs · retries · exponential backoff · scheduling
          </p>
        </div>
      </header>

      <section className="grid grid-cols-2 md:grid-cols-6 gap-3 mb-8">
        <Chip label="Workers" value={snap?.workers.workers ?? 0} />
        <Chip label="Pending" value={c.pending ?? 0} />
        <Chip label="Running" value={c.running ?? 0} accent="text-amber-300" />
        <Chip label="Succeeded" value={c.succeeded ?? 0} accent="text-emerald-300" />
        <Chip label="Dead" value={c.dead ?? 0} accent="text-rose-300" />
        <Chip label="Processed" value={snap?.workers.processed ?? 0} />
      </section>

      <section className="bg-slate-900/50 border border-slate-800 rounded-2xl p-5 mb-8">
        <h2 className="text-sm uppercase tracking-widest text-slate-400 mb-4">Enqueue Ticket Emails</h2>
        <div className="flex flex-wrap gap-3 items-center">
          <button
            onClick={() => enqueue(ticket())}
            className="bg-teal-400 hover:bg-teal-300 text-slate-900 font-medium rounded-lg px-4 py-2 text-sm"
          >
            Sell 1 ticket
          </button>
          <button
            onClick={async () => {
              for (let i = 0; i < 20; i++) await enqueue(ticket());
            }}
            className="bg-emerald-500 hover:bg-emerald-400 text-slate-900 font-medium rounded-lg px-4 py-2 text-sm"
          >
            Sell 20 tickets (burst)
          </button>
          <button
            onClick={() => enqueue({ ...ticket(), type: "generate_report", delay_secs: 5, payload: { report: "daily_sales" } })}
            className="bg-cyan-500 hover:bg-cyan-400 text-slate-900 font-medium rounded-lg px-4 py-2 text-sm"
          >
            Schedule reminder (+5s)
          </button>
          <span className="text-xs text-slate-400">~35% of emails "fail" to demo retries &amp; backoff.</span>
        </div>
      </section>

      <section>
        <h2 className="text-sm uppercase tracking-widest text-slate-400 mb-3">Recent Jobs</h2>
        <div className="overflow-hidden rounded-2xl border border-slate-800">
          <table className="w-full text-sm">
            <thead className="bg-slate-900/80 text-slate-400">
              <tr>
                <th className="text-left px-4 py-3 font-medium">Type</th>
                <th className="text-left px-4 py-3 font-medium">State</th>
                <th className="text-left px-4 py-3 font-medium">Attempts</th>
                <th className="text-left px-4 py-3 font-medium">Last error</th>
                <th className="text-left px-4 py-3 font-medium">Updated</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800">
              {(snap?.queue.recent ?? []).length === 0 ? (
                <tr>
                  <td colSpan={5} className="px-4 py-8 text-center text-slate-500">
                    No jobs yet — sell a ticket above.
                  </td>
                </tr>
              ) : (
                snap?.queue.recent.map((j) => (
                  <tr key={j.id} className="hover:bg-slate-900/40">
                    <td className="px-4 py-3 font-mono text-teal-200">{j.type}</td>
                    <td className="px-4 py-3">
                      <span className={`text-xs px-2 py-0.5 rounded-full ${stateStyle[j.state]}`}>{j.state}</span>
                    </td>
                    <td className="px-4 py-3 font-mono">
                      {j.attempts}/{j.max_attempts}
                    </td>
                    <td className="px-4 py-3 text-xs text-rose-300/80 truncate max-w-xs">{j.last_error ?? ""}</td>
                    <td className="px-4 py-3 text-xs text-slate-500 font-mono">
                      {new Date(j.updated_at).toLocaleTimeString()}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
