"""Assemble BrowserOS Neo captures, frame-1.png through frame-5.png.

Optional authoring dependency: Pillow. Not needed by the SQL Knowledge runtime.
Capture the four numbered HTML figures, then write-cycle.html, at CSS scale 1.
"""
import argparse
from pathlib import Path
from PIL import Image

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('capture_directory', type=Path)
args = parser.parse_args()
frames = []
for index in range(1, 6):
    with Image.open(args.capture_directory / f'frame-{index}.png') as source:
        if source.width < 960 or source.height < 600:
            raise ValueError('Capture must contain the complete 960x600 figure at CSS scale 1')
        frames.append(source.crop((0, 0, 960, 600)).convert('RGB'))
destination = Path(__file__).resolve().parents[1] / 'docs/images/write-cycle.gif'
frames[0].save(destination, save_all=True, append_images=frames[1:],
               duration=[1200, 1200, 1200, 1200, 2400], disposal=2, optimize=True)
print(destination)
