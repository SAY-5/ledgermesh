import { Hero } from "./components/Hero.tsx";
import "./styles/map.css";
import "./styles/hero.css";

export function App() {
  return (
    <>
      <a className="skip-link" href="#saga">
        Skip to the demos
      </a>
      <Hero />
      <main id="main" />
    </>
  );
}
