import { Hero } from "./components/Hero.tsx";
import { SagaTrace } from "./components/SagaTrace.tsx";
import "./styles/map.css";
import "./styles/hero.css";
import "./styles/saga.css";

export function App() {
  return (
    <>
      <a className="skip-link" href="#saga">
        Skip to the demos
      </a>
      <Hero />
      <main id="main">
        <SagaTrace />
      </main>
    </>
  );
}
