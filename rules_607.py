import pglast
from pglast import Node, parse_sql
from pglast.ast import ColumnRef, SelectStmt, SubLink
from pglast.visitors import Visitor
from pglast.stream import RawStream
import copy

"""
# 607: String hashed subplan

Warning:
This rule maybe specific to PSQL and requires schema information.
The columns involved need to have string type.

Idea: 
Where condition of the form 
  WHERE ca1 IN/NOT IN (SELECT cb1 from b)
often triggers an optimization called "Hashed SubPlan": the subquery (SELECT cb1 from b)
does not depend on the outer select statement, so we can put the content of the subquery
in a hash set to answer the outer IN/NOT IN query.

It is observed that the optimization works well if the columns ca1 and cb1 are integers, 
but not so well if they are strings. A way to remind PSQL optimizer is to add a GROUP BY:
  WHERE ca1 IN/NOT IN (SELECT cb1 from b GROUP BY cb1)

Another way to do it is to use EXISTS, but we shall not consider it at this point.

Assumption: each column is the form of table.column
"""


class StringHashedReWrite():

    def __init__(self):
        self.results = []

    def __call__(self, root):
        self.root = root
        StringHashed = self.StringHashed()
        self.results.append(StringHashed(self.rootCopy()))
        return self.results

    def rootCopy(self):
        return parse_sql(RawStream()(self.root))

    class StringHashed(Visitor):
        def __init__(self):
            pass

        def visit_SubLink(self, ancestors, node: SubLink):
            # we need either col IN (subquery) or col = ANY (subquery)
            if node.subLinkType is not pglast.enums.primnodes.SubLinkType.ANY_SUBLINK:
                return
            if node.operName and node.operName[0].val != '=':
                return
            subPlan: SelectStmt = node.subselect
            if subPlan.groupClause:
                return

            # we check that the subquery is self-contained, i.e. does not use exterior table
            check = self.CheckSelfContain(subPlan, [])
            check(subPlan)
            if (check.selfContain):
                groupByCol = copy.deepcopy(subPlan.targetList[0].val)
                groupByCol.location = -1
                subPlan.groupClause = (groupByCol, )

        class CheckSelfContain(Visitor):
            """ Takes in a SelectStmt node and report whether it doesn't use exterior tables"""

            def __init__(self, selectNode: SelectStmt, tables: list):
                self.selfContain = True
                self.root = selectNode
                # interior tables
                self.tables = tables
                tablesThisLayer = self.TablesThisLayer()
                tablesThisLayer(selectNode.fromClause)
                self.tables = self.tables + tablesThisLayer.tables

            def visit_ColumnRef(self, ancestors, node: ColumnRef):
                # ASSUMPTION: each column is explicitly associated with its table, i.e. table.column
                if (node.fields[0].val not in self.tables):
                    self.selfContain = False

            def visit_SelectStmt(self, ancestors, node):
                if (node is self.root):
                    return pglast.visitors.Continue()
                # If we find a sub-select, check within that, and then skip diving into it
                self.selfContain = self.selfContain and type(
                    self)(node, self.tables)(node)
                return pglast.visitors.Skip()

            class TablesThisLayer(Visitor):
                """ Takes in a fromClause and get table names (not including tables in subqueries that appear in from statement) """

                def __init__(self):
                    self.tables = []

                def visit_RangeVar(self, ancestors, node):
                    self.tables.append(
                        node.alias.aliasname if node.alias else node.relname)

                def visit_SelectStmt(self, ancestors, node):
                    return pglast.visitors.Skip()


# Just for demo
sql = "SELECT DISTINCT s.name FROM orders AS o RIGHT JOIN salesperson AS s using (sales_id) WHERE ((sales_id) NOT IN ((SELECT sales_id FROM orders INNER JOIN company using (com_id) WHERE (name = 'red'))))"
root = Node(parse_sql(sql))
StringHashedWriter = StringHashedReWrite()
res = StringHashedWriter(root)
print(RawStream()(res[0]))
