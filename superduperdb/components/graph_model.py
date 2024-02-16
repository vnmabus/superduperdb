from __future__ import annotations
from superduperdb.components.model import Model, _Predictor
import typing as t
import dataclasses as dc

if t.TYPE_CHECKING:
    from superduperdb.base.datalayer import Datalayer
    from superduperdb.backends.base.query import CompoundSelect
    from superduperdb.jobs.job import Job

# t.Self can only be used from 3.11 onwards
Self = t.TypeVar("Self")


@dc.dataclass(kw_only=True, frozen=True)
class _Node:
    """A node of the graph and its inputs."""
    predictor: _Predictor
    inputs: t.Sequence[_Predictor]
    accepts_data: bool


@dc.dataclass(kw_only=True)
class GraphModel(Model):
    """
    Model that implements a directed acyclic graph (DAG) of models.

    Each model in the graph may receive inputs for models already in the graph.

    Examples:
        (Incomplete) example of how it would be used

        We first create the model
        >>> graph = GraphModel()

        We can add nodes that receive the input from predict as
        >>> graph.add_node(m1)

        We can add a node that receives input from another node
        as well as from predict itself as
        >>> graph.add_node(m2, inputs=(m1,), accepts_data=True)

        We can add a node that only receives input from other nodes
        >>> graph.add_node(m3, inputs=(m1, m2))
    """

    # I do not need this right now
    object: None = None

    def __post_init__(self, artifacts: t.Any) -> None:
        # I want to check identity in this mapping. Predictors may have
        # redefined equality, so lets have some caution here
        self._nodes: t.Dict[int, _Node] = {}
        super().__post_init__(artifacts)

    def add_node(
        self: Self,
        predictor: _Predictor,
        inputs: t.Sequence[_Predictor] = (),
        accepts_data: bool | t.Literal["auto"] = "auto",
    ) -> Self:
        """
        Adds a node and its input connections to the graph.

        Args:
            predictor: Predictor to add.
            inputs: The predictors whose outputs are the inputs of this
                predictor.
            accepts_data: Whether the predictor receives also the original
                data passed to ``predict``. The default, "auto", is that
                the original data is only passed if there are no other
                inputs.

        Returns:
            Self, for easy chaining.

        """
        if self._nodes.get(id(predictor)) is not None:
            raise ValueError(
                f"Predictor already in graph: {predictor}",
            )

        for i in inputs:
            if self._nodes.get(id(i)) is None:
                raise ValueError(
                    f"Input not in graph: {i}",
                )

        if accepts_data == "auto":
            accepts_data = not inputs

        self._nodes[id(predictor)] = _Node(
            predictor=predictor,
            inputs=inputs,
            accepts_data=accepts_data,
        )

        return self

    # The signature is incompatible with the superclass
    # because the parameters should have been kw-only, IMHO
    def predict(
        self,
        X: t.Any,
        db: t.Optional[Datalayer] = None,
        select: t.Optional[CompoundSelect] = None,
        ids: t.Optional[t.List[str]] = None,
        max_chunk_size: t.Optional[int] = None,
        dependencies: t.Sequence[Job] = (),
        **kwargs: t.Any,
    ) -> t.Any:

        # I do not know exactly which method should I override, and which one
        # should I call, as there are no docstrings. I will assume for
        # illustration purposes that it is ok to override predict and call
        # predict inside.
        # I will assume that I can reuse code like in
        # https://docs.superduperdb.com/docs/docs/walkthrough/linking_interdependent_models
        # somehow.

        # Lets cache the outputs at each step
        _outputs: t.Dict[int, t.Any] = {}

        # As the nodes were inserted in topological order, I
        # can do a simple loop here
        for node in self._nodes.values():

            # TODO: What if select is None?
            node_select = (
                select
                if node.accepts_data
                else select  # ??? TODO: Create empty select?
            )

            # Add previous outputs in select and as dependencies
            node_deps = list(dependencies)
            for i in node.inputs:
                node_select = node_select.outputs(X, i.identifier)
                node_deps.append(_outputs[id(i)])

            # TODO: Check if more parameters of predict should be modified
            output = node.predictor.predict(
                X=X,
                db=db,
                select=node_select,
                ids=ids,
                max_chunk_size=max_chunk_size,
                dependencies=node_deps,
                **kwargs,
            )

            _outputs[id(node.predictor)] = output

        # It is not clear what I should return if there are several "sink"
        # nodes. Lets assume that the output of the last node, which is
        # definitly a sink.
        return output


######
# TODO: There are several remaining things:
#     - Complete the implementation in predict
#     - Check any other methods that need to be implemented
#       (fit, apredict, ...?)
#     - Add tests
#     - Improve the docstrings adding/completing doctests and see also.
#     - Describe the functionality in the documentation, maybe adding an
#       example too.
#
# However, I hope that this exercise is enough to evaluate what I can
# contribute to the team, before receiving any kind of mentorship. In a
# real-world situation I would expect to be able to advance faster by
# asking questions to coworkers and/or receiving mentorship from core
# developers.
######
