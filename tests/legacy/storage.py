from sql_knowledge._core import storage as s
import memory,sys
s.m=memory
sys.modules[__name__]=s
