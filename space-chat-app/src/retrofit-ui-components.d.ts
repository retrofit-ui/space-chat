declare module "@retrofit-ui/spa-solid-shoelace/components" {
  import type { RootSpec, TextSpec, TabsSpec, DetailsSpec } from "@retrofit-ui/core";
  import type { JSX } from "solid-js";

  export function SpecRenderer(props: {
    spec: RootSpec | TextSpec | TabsSpec | DetailsSpec;
    apiBase: string;
  }): JSX.Element;
}
