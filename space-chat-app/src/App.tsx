import { createSignal, type Component } from "solid-js";
import { invoke } from "@tauri-apps/api/core";

const App: Component = () => {
  const [greeting, setGreeting] = createSignal("");

  const sayHello = async () => {
    const result = await invoke<string>("greet", { name: "Alice" });
    setGreeting(result);
  };

  return (
    <div>
      <h1>space-chat</h1>
      <button onClick={sayHello}>Say hello</button>
      <p data-testid="greeting">{greeting()}</p>
    </div>
  );
};

export default App;
