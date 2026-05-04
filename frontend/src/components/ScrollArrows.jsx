import React, { useCallback, useEffect, useRef, useState } from "react";

/**
 * ScrollArrows — wraps a horizontally-scrollable table container.
 *
 * Renders the inner div with class "prem-scanner-wrap" and adds a sticky
 * bottom bar containing ◀ ▶ arrow buttons.  The arrows fade when the scroll
 * is at the boundary (fully left or fully right) and disappear entirely when
 * the table content fits without scrolling.
 *
 * Usage:
 *   <ScrollArrows>
 *     <table className="prem-scanner-table">…</table>
 *   </ScrollArrows>
 */
export default function ScrollArrows({ children }) {
  const wrapRef = useRef(null);
  const [canLeft, setCanLeft]   = useState(false);
  const [canRight, setCanRight] = useState(false);

  const update = useCallback(() => {
    const el = wrapRef.current;
    if (!el) return;
    const atLeft  = el.scrollLeft <= 2;
    const atRight = el.scrollLeft + el.clientWidth >= el.scrollWidth - 2;
    setCanLeft(!atLeft);
    setCanRight(!atRight);
  }, []);

  useEffect(() => {
    const el = wrapRef.current;
    if (!el) return;
    update();
    el.addEventListener("scroll", update, { passive: true });
    const ro = new ResizeObserver(update);
    ro.observe(el);
    return () => {
      el.removeEventListener("scroll", update);
      ro.disconnect();
    };
  }, [update]);

  const scrollBy = (dx) => {
    wrapRef.current?.scrollBy({ left: dx, behavior: "smooth" });
  };

  const showBar = canLeft || canRight;

  return (
    <div style={{ position: "relative" }}>
      <div ref={wrapRef} className="prem-scanner-wrap">
        {children}
      </div>
      {showBar && (
        <div className="scroll-arrows-bar">
          <button
            className={`scroll-arrow-btn${canLeft ? "" : " scroll-arrow-disabled"}`}
            onClick={() => scrollBy(-260)}
            disabled={!canLeft}
            aria-label="Scroll left"
          >◀</button>
          <button
            className={`scroll-arrow-btn${canRight ? "" : " scroll-arrow-disabled"}`}
            onClick={() => scrollBy(260)}
            disabled={!canRight}
            aria-label="Scroll right"
          >▶</button>
        </div>
      )}
    </div>
  );
}
