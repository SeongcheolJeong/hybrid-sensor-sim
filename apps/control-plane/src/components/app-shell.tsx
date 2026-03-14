import { Link, Outlet, useRouterState } from "@tanstack/react-router";

const NAV_ITEMS = [
  { to: "/", label: "Workspace", caption: "Operational status" },
  { to: "/projects", label: "Projects", caption: "Projects and assets" },
  { to: "/simulation", label: "Simulation", caption: "Object sim and replay" },
  { to: "/validation", label: "Validation", caption: "Batch and probe sets" },
  { to: "/maps", label: "Maps", caption: "Map assets and routes" },
  { to: "/runtime", label: "Runtime", caption: "AWSIM and backend strategy" },
  { to: "/autoware", label: "Autoware", caption: "Ingest contracts" },
  { to: "/history", label: "History", caption: "Migration and provenance" },
];

export function AppShell() {
  const pathname = useRouterState({ select: (state) => state.location.pathname });

  return (
    <div className="min-h-screen bg-cp-bg text-cp-text">
      <div className="mx-auto grid min-h-screen max-w-[1600px] grid-cols-1 lg:grid-cols-[280px_minmax(0,1fr)]">
        <aside className="border-b border-cp-border bg-cp-panel/95 px-6 py-6 lg:border-b-0 lg:border-r">
          <div className="mb-8">
            <div className="text-xs uppercase tracking-[0.24em] text-cp-accent">Hybrid Sensor Sim</div>
            <div className="mt-2 font-display text-2xl font-semibold">Control Plane</div>
            <div className="mt-2 text-sm text-cp-text-muted">Applied-style internal operations surface for simulation, runtime, and Autoware readiness.</div>
          </div>
          <nav className="space-y-2">
            {NAV_ITEMS.map((item) => {
              const active = pathname === item.to || (item.to !== "/" && pathname.startsWith(item.to));
              return (
                <Link
                  key={item.to}
                  to={item.to}
                  className={`block rounded-2xl border px-4 py-3 transition ${
                    active
                      ? "border-cp-accent/40 bg-cp-accent/10"
                      : "border-transparent bg-transparent hover:border-cp-border hover:bg-cp-surface/70"
                  }`}
                >
                  <div className="text-sm font-semibold text-cp-text">{item.label}</div>
                  <div className="mt-1 text-xs text-cp-text-muted">{item.caption}</div>
                </Link>
              );
            })}
          </nav>
        </aside>
        <main className="px-5 py-6 sm:px-6 lg:px-8">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
