from sql_knowledge._core import mcp_server as s
from pathlib import Path
import memory
s.ROOT=Path(__file__).resolve().parents[2];s.memory=memory;s.storage.m=memory
if __name__=='__main__':s.main()
