import * as Primitive from "@radix-ui/react-progress";

export function Progress({ value, tone = "info", label }: { value: number; tone?: "info" | "success" | "warning"; label: string }) {
  return <Primitive.Root className={`progress progress--${tone}`} value={value} aria-label={label}><Primitive.Indicator className="progress__indicator" style={{ transform: `translateX(-${100 - Math.min(100, Math.max(0, value))}%)` }} /></Primitive.Root>;
}

