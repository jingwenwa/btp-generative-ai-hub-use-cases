# pylint: disable=missing-module-docstring
# pylint: disable=consider-using-f-string
# pylint: disable=duplicate-code
from pandas import DataFrame

from .algorithm_base import AlgorithmBase
from .. import Graph


class CommunitiesLouvain(AlgorithmBase):
    """
    Identifies communities.

    ...

    Examples
    --------
    >>> import hana_ml.graph.algorithms as hga
    >>> comm = hga.CommunitiesLouvain(graph=hana_graph).execute(runs=2, weight='LENGTH')
    >>> print("Communities:", comm.communities_count)
    >>> print("Modularity:", comm.modularity)
    >>> print("Communities Histogram\n", comm.communities)
    >>> print("Vertices\n", comm.vertices)
    """

    def __init__(self, graph: Graph):
        super().__init__(graph)

        self._graph_script = """
            DO (
	            IN i_runs INT => {runs},
                OUT o_vertices TABLE({vertex_columns}, "$COMMUNITY" BIGINT) => ?,
                OUT o_hist TABLE("COMMUNITY" BIGINT, "NUMBER_OF_VERTICES" BIGINT) => ?,
                OUT o_scalars TABLE ("NUMBER_OF_COMMUNITIES" BIGINT, "MODULARITY" DOUBLE) => ?
            )
            LANGUAGE GRAPH
            BEGIN
                GRAPH g = Graph("{schema}", "{workspace}");
                SEQUENCE<MULTISET<VERTEX>> communities = COMMUNITIES_LOUVAIN(:g, :i_runs {weighted_definition} );
                -- counting the elements in the communites SEQUENCE provides the number of communities found
                o_scalars."NUMBER_OF_COMMUNITIES"[1L] = COUNT(:communities);
                o_scalars."MODULARITY"[1L] = MODULARITY(:g, :communities);
                -- to get the number of vertices in each community, we count the elements in each community MULTISET, and write the results in the output table
                BIGINT i = 0L;
                FOREACH community IN :communities {{
                    i = :i + 1L;
                    o_hist."COMMUNITY"[:i] = :i - 1L;
                    o_hist."NUMBER_OF_VERTICES"[:i] = COUNT(:community);
                }}
                -- and finally project the result: return the community id for each vertex
                MAP<VERTEX, BIGINT> m_vertexCommunity = TO_ORDINALITY_MAP(:communities);
                o_vertices = SELECT {vertex_select}, :m_vertexCommunity[:v] FOREACH v in VERTICES(:g);
            END;
        """

        self._graph_script_vars = {
            "runs": ("runs", False, int, 1),
            "weight": ("weight", False, str, None),
            "schema": (None, False, str, self._graph.workspace_schema),
            "workspace": (None, False, str, self._graph.workspace_name),
            "vertex_dtype": (None, False, str, self._graph.vertex_key_col_dtype),
            "vertex_columns": (None, False, str, self._default_vertex_cols()),
            "vertex_select": (None, False, str, self._default_vertex_select("v")),
        }

    def _process_parameters(self, arguments):
        super()._process_parameters(arguments)

        # Construct the Weight part of the SQL Statement depending
        # on if a weight parameter was provided or not
        if self._templ_vals["weight"]:
            self._templ_vals[
                "weighted_definition"
            ] = """
                    , (Edge e) => DOUBLE{{ return DOUBLE(:e."{weight}"); }}
                """.format(weight=self._templ_vals["weight"])
        else:
            self._templ_vals[
                "weighted_definition"
            ] = """
                """

    def _validate_parameters(self):
        # Version check
        super()._validate_parameters()

        # Check Runs
        if self._templ_vals["runs"] < 1:
            raise ValueError(
                "Parameter Runs needs to be greater than 0"
            )

    def execute(self, runs: int = 1, weight: str = None,) -> "CommunitiesLouvain":  # pylint: disable=arguments-differ, useless-super-delegation
        """
        Executes the communities.

        Returns
        -------
        Communities
            Communities object instance
        """
        return super(CommunitiesLouvain, self).execute(
            runs=runs,
            weight=weight,
        )

    @property
    def vertices(self) -> DataFrame:
        """
        Returns
        -------
        pandas.Dataframe
            A Pandas `DataFrame` that contains the vertex keys and the community assignment.
        """
        vertex_cols = [
            col
            for col in self._graph.vertices_hdf.columns
            if col in self._default_vertices_column_filter
        ]
        vertex_cols.append("$COMMUNITY")

        return DataFrame(self._results.get("o_vertices", None), columns=vertex_cols)

    @property
    def communities(self) -> DataFrame:
        """
        Returns
        -------
        pandas.Dataframe
            A Pandas `DataFrame` that contains communities and
            number of vertices in each community.
        """
        vertex_cols = ["COMMUNITY", "NUMBER_OF_VERTICES"]

        return DataFrame(self._results.get("o_hist", None), columns=vertex_cols)

    @property
    def communities_count(self) -> int:
        """
        Returns
        -------
        int
            The number of communities in the graph.
        """
        scalars = self._results.get("o_scalars", None)

        if scalars is None:
            return 0
        else:
            return scalars[0][0]

    @property
    def modularity(self) -> float:
        """
        Returns
        -------
        double
            The modularity of the communities.
        """
        scalars = self._results.get("o_scalars", None)

        if scalars is None:
            return 0.0
        else:
            return scalars[0][1]
