import { forwardRef, type ButtonHTMLAttributes } from "react";
import { LoaderCircle } from "lucide-react";
import { cn } from "../../lib/utils";

type Props = ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "primary" | "secondary" | "ghost" | "danger";
  size?: "sm" | "md" | "icon";
  loading?: boolean;
};

export const Button = forwardRef<HTMLButtonElement, Props>(function Button(
  { className, variant = "secondary", size = "md", loading, disabled, children, ...props }, ref,
) {
  return (
    <button ref={ref} className={cn("button", `button--${variant}`, `button--${size}`, className)} disabled={disabled || loading} {...props}>
      {loading && <LoaderCircle size={15} className="spin" aria-hidden />}
      {children}
    </button>
  );
});

