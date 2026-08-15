import { useEffect, useRef } from "react";

export function HistoryLoader({ hasMore, loading, onLoadMore, label = "runs" }: { hasMore: boolean; loading: boolean; onLoadMore: () => unknown; label?: "runs" | "checks" }) {
  const trigger = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!hasMore || !trigger.current || !("IntersectionObserver" in window)) return;
    const observer = new IntersectionObserver(([entry]) => {
      if (entry?.isIntersecting && !loading) void onLoadMore();
    }, { rootMargin: "240px" });
    observer.observe(trigger.current);
    return () => observer.disconnect();
  }, [hasMore, loading, onLoadMore]);

  if (!hasMore) return null;
  return <button ref={trigger} className="history-loader" disabled={loading} onClick={() => void onLoadMore()}>{loading ? `Loading older ${label}…` : `Load older ${label}`}</button>;
}
