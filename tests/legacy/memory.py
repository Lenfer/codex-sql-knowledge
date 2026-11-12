from sql_knowledge._core import memory as m
from pathlib import Path
import sys
m.ROOT=Path(__file__).resolve().parents[2]
if __name__=='__main__':sys.exit(m.main())
else:sys.modules[__name__]=m
