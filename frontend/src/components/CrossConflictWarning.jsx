import React, { useEffect, useRef, useState } from "react";

const MSG = "Golden cross with downtrend — the 50 SMA just crossed above the 200 SMA but price is still falling. The cross is technically bullish but the price action hasn't confirmed it. This is a higher risk setup. The cross could fail if price continues lower.";

export default function CrossConflictWarning() {
  const [open, setOpen] = useState(false);
  const ref = useRef(null);

  useEffect(() => {
    if (!open) return;
    function onDown(e) {
      if (ref.current && !ref.current.contains(e.target)) setOpen(false);
    }
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);

  return (
    <span
      ref={ref}
      className="cross-warn-wrap"
      onClick={e => { e.stopPropagation(); setOpen(v => !v); }}
    >
      <span className="cross-warn-icon">⚠️</span>
      {open && <div className="cross-warn-popup">{MSG}</div>}
    </span>
  );
}
