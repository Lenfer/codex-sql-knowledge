# README figures

The architecture figure separates agent clients, the local writer and the project
database. The write cycle follows a request through validation, commit and receipt.
The GIF highlights those steps once and finishes with the complete static figure.
All labels remain visible in each frame. The adjacent SVG provides a still image.

These are schematic illustrations, not recordings of a terminal session. Colors
use warm paper, dark ink and a clay accent. System fonts keep the sources usable
offline: Georgia for titles, Arial/Helvetica for labels and Menlo/Consolas for code.
They make no external font requests.

Run `python3 scripts/build_figures.py` from the repository root to regenerate HTML.
Export `architecture.html` and `write-cycle.html` using Diagram Design's
`export_svg.py --system-fonts` helper. The numbered `write-step-*.html` files are
deterministic frames for browser capture. The final frame is `write-cycle.html`.
Capture at CSS scale 1 with the complete figure visible. Name captures
`frame-1.png` through `frame-5.png`, then run
`python3 scripts/assemble_gif.py /path/to/captures` with Pillow available. The
assembler crops the figure at the page origin and writes a GIF that plays once.

| Source | README asset |
| --- | --- |
| [architecture.html](architecture.html) | [architecture.svg](../images/architecture.svg) |
| [write-cycle.html](write-cycle.html) | [write-cycle.svg](../images/write-cycle.svg) |
| `write-step-1.html` through `write-step-4.html` | [write-cycle.gif](../images/write-cycle.gif) |

Diagram Design is an authoring dependency, not part of the SQL Knowledge runtime.
