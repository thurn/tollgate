import * as Primitive from "@radix-ui/react-tooltip";
import type { ReactNode } from "react";

export function Tooltip({ label, children, side = "top" }: { label: ReactNode; children: ReactNode; side?: "top" | "right" | "bottom" | "left" }) {
  return (
    <Primitive.Root>
      <Primitive.Trigger asChild>{children}</Primitive.Trigger>
      <Primitive.Portal>
        <Primitive.Content className="tooltip" side={side} sideOffset={7}>
          {label}<Primitive.Arrow className="tooltip__arrow" />
        </Primitive.Content>
      </Primitive.Portal>
    </Primitive.Root>
  );
}

