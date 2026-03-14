import { createRootRoute, createRoute, createRouter } from "@tanstack/react-router";

import { AppShell } from "./components/app-shell";
import { AutowarePage } from "./pages/autoware-page";
import { HistoryPage } from "./pages/history-page";
import { MapsPage } from "./pages/maps-page";
import { ProjectsPage } from "./pages/projects-page";
import { RunDetailPage } from "./pages/run-detail-page";
import { RuntimePage } from "./pages/runtime-page";
import { SimulationPage } from "./pages/simulation-page";
import { ValidationPage } from "./pages/validation-page";
import { WorkspacePage } from "./pages/workspace-page";

const rootRoute = createRootRoute({ component: AppShell });

const workspaceRoute = createRoute({ getParentRoute: () => rootRoute, path: "/", component: WorkspacePage });
const projectsRoute = createRoute({ getParentRoute: () => rootRoute, path: "/projects", component: ProjectsPage });
const simulationRoute = createRoute({ getParentRoute: () => rootRoute, path: "/simulation", component: SimulationPage });
const validationRoute = createRoute({ getParentRoute: () => rootRoute, path: "/validation", component: ValidationPage });
const mapsRoute = createRoute({ getParentRoute: () => rootRoute, path: "/maps", component: MapsPage });
const runtimeRoute = createRoute({ getParentRoute: () => rootRoute, path: "/runtime", component: RuntimePage });
const autowareRoute = createRoute({ getParentRoute: () => rootRoute, path: "/autoware", component: AutowarePage });
const historyRoute = createRoute({ getParentRoute: () => rootRoute, path: "/history", component: HistoryPage });
const runDetailRoute = createRoute({ getParentRoute: () => rootRoute, path: "/runs/$runId", component: RunDetailPage });

const routeTree = rootRoute.addChildren([
  workspaceRoute,
  projectsRoute,
  simulationRoute,
  validationRoute,
  mapsRoute,
  runtimeRoute,
  autowareRoute,
  historyRoute,
  runDetailRoute,
]);

export const router = createRouter({ routeTree });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
