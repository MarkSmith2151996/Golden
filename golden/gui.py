"""
Golden Lead Finder — Tkinter GUI.

Launch:  python -m golden.gui
"""

from __future__ import annotations

import json
import logging
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from .pipeline import run_pipeline
from .sources import list_cities

# Silence the noisy per-request httpx logs; keep pipeline-level info
logging.getLogger("httpx").setLevel(logging.WARNING)


class GoldenApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Golden Lead Finder")
        self.geometry("960x700")
        self.minsize(800, 550)

        self._results: list[dict] = []
        self._city_vars: dict[str, tk.BooleanVar] = {}

        self._build_controls()
        self._build_status()
        self._build_table()
        self._build_detail()

    # ── Controls ─────────────────────────────────────────────────────

    def _build_controls(self) -> None:
        frame = ttk.LabelFrame(self, text="Parameters", padding=8)
        frame.pack(fill="x", padx=8, pady=(8, 0))

        # City checkboxes
        city_frame = ttk.Frame(frame)
        city_frame.pack(fill="x")
        ttk.Label(city_frame, text="Cities:").pack(side="left")
        for city in list_cities():
            var = tk.BooleanVar(value=True)
            self._city_vars[city] = var
            ttk.Checkbutton(city_frame, text=city.title(), variable=var).pack(
                side="left", padx=4
            )

        # Parameter fields
        param_frame = ttk.Frame(frame)
        param_frame.pack(fill="x", pady=(6, 0))

        ttk.Label(param_frame, text="Days:").pack(side="left")
        self._days_var = tk.StringVar(value="90")
        ttk.Entry(param_frame, textvariable=self._days_var, width=6).pack(
            side="left", padx=(2, 12)
        )

        ttk.Label(param_frame, text="Limit:").pack(side="left")
        self._limit_var = tk.StringVar(value="")
        ttk.Entry(param_frame, textvariable=self._limit_var, width=6).pack(
            side="left", padx=(2, 12)
        )

        ttk.Label(param_frame, text="Min Severity:").pack(side="left")
        self._minsev_var = tk.StringVar(value="1")
        ttk.Entry(param_frame, textvariable=self._minsev_var, width=6).pack(
            side="left", padx=(2, 12)
        )

        # Buttons
        self._run_btn = ttk.Button(
            param_frame, text="Run Pipeline", command=self._on_run
        )
        self._run_btn.pack(side="right", padx=(12, 0))

        self._export_btn = ttk.Button(
            param_frame, text="Export JSON", command=self._on_export, state="disabled"
        )
        self._export_btn.pack(side="right")

    # ── Status bar ───────────────────────────────────────────────────

    def _build_status(self) -> None:
        self._status_var = tk.StringVar(value="Ready")
        bar = ttk.Label(self, textvariable=self._status_var, relief="sunken", anchor="w")
        bar.pack(fill="x", padx=8, pady=4)

    # ── Results table ────────────────────────────────────────────────

    def _build_table(self) -> None:
        container = ttk.Frame(self)
        container.pack(fill="both", expand=True, padx=8)

        cols = ("#", "Sev", "City", "Name", "Address", "Date", "Violations")
        self._tree = ttk.Treeview(
            container, columns=cols, show="headings", selectmode="browse"
        )
        for col in cols:
            self._tree.heading(col, text=col)

        self._tree.column("#", width=40, stretch=False)
        self._tree.column("Sev", width=40, stretch=False)
        self._tree.column("City", width=70, stretch=False)
        self._tree.column("Name", width=200)
        self._tree.column("Address", width=220)
        self._tree.column("Date", width=90, stretch=False)
        self._tree.column("Violations", width=70, stretch=False)

        vsb = ttk.Scrollbar(container, orient="vertical", command=self._tree.yview)
        self._tree.configure(yscrollcommand=vsb.set)

        self._tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self._tree.bind("<<TreeviewSelect>>", self._on_select)

    # ── Detail panel ─────────────────────────────────────────────────

    def _build_detail(self) -> None:
        frame = ttk.LabelFrame(self, text="Violation Details (click a row above)", padding=4)
        frame.pack(fill="x", padx=8, pady=(0, 8))

        self._detail = tk.Text(frame, height=8, wrap="word", state="disabled")
        self._detail.pack(fill="x")

    # ── Actions ──────────────────────────────────────────────────────

    def _on_run(self) -> None:
        cities = [c for c, v in self._city_vars.items() if v.get()]
        if not cities:
            messagebox.showwarning("No cities", "Select at least one city.")
            return

        try:
            days = int(self._days_var.get())
        except ValueError:
            messagebox.showerror("Invalid input", "Days must be an integer.")
            return

        limit_str = self._limit_var.get().strip()
        limit = int(limit_str) if limit_str else None
        if limit_str and limit is None:
            messagebox.showerror("Invalid input", "Limit must be an integer.")
            return

        try:
            min_sev = int(self._minsev_var.get())
        except ValueError:
            messagebox.showerror("Invalid input", "Min Severity must be an integer.")
            return

        self._run_btn.configure(state="disabled")
        self._export_btn.configure(state="disabled")
        self._status_var.set("Running...")
        self._clear_results()

        def worker() -> None:
            try:
                results = run_pipeline(
                    cities=cities, days=days, limit=limit, min_severity=min_sev
                )
                self.after(0, self._load_results, results)
            except Exception as exc:
                self.after(0, self._on_error, str(exc))

        threading.Thread(target=worker, daemon=True).start()

    def _load_results(self, results: list[dict]) -> None:
        self._results = results
        for i, lead in enumerate(results, 1):
            est = lead["establishment"]
            self._tree.insert(
                "",
                "end",
                iid=str(i),
                values=(
                    i,
                    lead["severity_score"],
                    lead.get("city", ""),
                    est["name"],
                    est["address"],
                    lead["latest_inspection_date"],
                    len(lead["relevant_violations"]),
                ),
            )
        self._status_var.set(f"Done: {len(results)} leads found")
        self._run_btn.configure(state="normal")
        if results:
            self._export_btn.configure(state="normal")

    def _on_error(self, msg: str) -> None:
        self._status_var.set(f"Error: {msg}")
        self._run_btn.configure(state="normal")

    def _clear_results(self) -> None:
        self._tree.delete(*self._tree.get_children())
        self._detail.configure(state="normal")
        self._detail.delete("1.0", "end")
        self._detail.configure(state="disabled")

    def _on_select(self, _event: tk.Event) -> None:  # type: ignore[type-arg]
        sel = self._tree.selection()
        if not sel:
            return
        idx = int(sel[0]) - 1
        lead = self._results[idx]

        lines: list[str] = []
        for v in lead["relevant_violations"]:
            lines.append(
                f"- Code: {v['violation_code']}  Type: {v['violation_type']}"
            )
            lines.append(f"  {v['violation_description']}")
            if v.get("problem_description"):
                lines.append(f"  Comments: {v['problem_description']}")
            lines.append("")

        self._detail.configure(state="normal")
        self._detail.delete("1.0", "end")
        self._detail.insert("1.0", "\n".join(lines) if lines else "(no violations)")
        self._detail.configure(state="disabled")

    def _on_export(self) -> None:
        if not self._results:
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("JSON files", "*.json"), ("All files", "*.*")],
        )
        if not path:
            return
        with open(path, "w") as f:
            json.dump(self._results, f, indent=2)
        self._status_var.set(f"Exported {len(self._results)} leads to {path}")


def main() -> None:
    app = GoldenApp()
    app.mainloop()


if __name__ == "__main__":
    main()
