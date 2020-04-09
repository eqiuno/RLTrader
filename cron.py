import sched

from lib.data.providers import DBDataProvider

store = DBDataProvider('huobipro', 'data/huobipro.db', '1m')
store.update()


