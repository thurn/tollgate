import type { ReactNode } from "react";
import { motion } from "framer-motion";

export function Segmented<T extends string>({ value, onChange, items, label }: { value: T; onChange: (value: T) => void; items: { value: T; label: ReactNode }[]; label: string }) {
  return <div className="segmented" role="radiogroup" aria-label={label}>{items.map((item) => <button key={item.value} role="radio" aria-checked={value === item.value} onClick={() => onChange(item.value)}>{value === item.value && <motion.span layoutId={`segmented-${label}`} className="segmented__selection" transition={{ type: "spring", stiffness: 500, damping: 36 }} />}<span>{item.label}</span></button>)}</div>;
}

