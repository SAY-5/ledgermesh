import { Hero } from "./components/Hero.tsx";
import { ChaosRun } from "./components/ChaosRun.tsx";
import { Footer } from "./components/Footer.tsx";
import { IdempotencyCache } from "./components/IdempotencyCache.tsx";
import { SagaTrace } from "./components/SagaTrace.tsx";
import "./styles/map.css";
import "./styles/hero.css";
import "./styles/saga.css";
import "./styles/resilience.css";
import "./styles/chaos.css";
import "./styles/footer.css";

export function App() {
  return (
    <>
      <a className="skip-link" href="#saga">
        Skip to the demos
      </a>
      <Hero />
      <main id="main">
        <SagaTrace />
        <IdempotencyCache />
        <ChaosRun />
      </main>
      <Footer />
    </>
  );
}
