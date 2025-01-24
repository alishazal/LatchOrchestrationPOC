from typing import List, Tuple, Optional


###############################################################################
# Constraint Models (Developer Side)
###############################################################################

class TaskConstraints:
    def __init__(
        self,
        max_spawn_count: Optional[int] = None,
        valid_next_nodes_policy: Optional[str] = 'allow_all',
        valid_next_nodes: Optional[List[str]] = [],
        valid_previous_nodes_policy: Optional[str] = 'allow_all',
        valid_previous_nodes: Optional[List[str]] = [],
        valid_incoming_edges_policy: Optional[str] = 'allow_all',
        valid_incoming_edges: Optional[List[Tuple[str, str]]] = [],
        valid_outgoing_edges_policy: Optional[str] = 'allow_all',
        valid_outgoing_edges: Optional[List[Tuple[str, str]]] = [],
    ):
        self.max_spawn_count = max_spawn_count

        # Valid nodes that the current node can spawn
        self.valid_next_nodes_policy = valid_next_nodes_policy
        self.valid_next_nodes = valid_next_nodes

        # Valid nodes that the current node can be spawned from
        self.valid_previous_nodes_policy = valid_previous_nodes_policy
        self.valid_previous_nodes = valid_previous_nodes

        # Valid incoming edges that the current node can have
        self.valid_incoming_edges_policy = valid_incoming_edges_policy
        self.valid_incoming_edges = valid_incoming_edges

        # Valid outgoing edges that the current node can have
        self.valid_outgoing_edges_policy = valid_outgoing_edges_policy
        self.valid_outgoing_edges = valid_outgoing_edges

        self.validate()

    def validate(self):
        """
        Validate the constraints
        """
        if self.max_spawn_count and self.max_spawn_count < 0:
            raise ValueError(f"max_spawn_count must be >= 0")

        self.validate_policy_values()
        self.validate_policy_nodes()

    def validate_policy_values(self):
        """
        Validate the policy values
        """
        valid_policies = ['allow_all', 'allow_none', 'custom']
        if self.valid_next_nodes_policy not in valid_policies:
            raise ValueError(f"valid_next_nodes_policy must be one of {valid_policies}")
        
        if self.valid_previous_nodes_policy not in valid_policies:
            raise ValueError(f"valid_previous_nodes_policy must be one of {valid_policies}")
        
        if self.valid_incoming_edges_policy not in valid_policies:
            raise ValueError(f"valid_incoming_edges_policy must be one of {valid_policies}")
        
        if self.valid_outgoing_edges_policy not in valid_policies:
            raise ValueError(f"valid_outgoing_edges_policy must be one of {valid_policies}")
        
    def validate_policy_nodes(self):
        """
        Validate the policy nodes
        """
        if self.valid_next_nodes_policy == 'allow_none' and self.valid_next_nodes:
            raise ValueError(f"allow_none valid_next_nodes_policy cannot take a list of valid_next_nodes")
        elif self.valid_next_nodes_policy == 'custom' and not self.valid_next_nodes:
            raise ValueError(f"custom valid_next_nodes_policy policy requires a list of valid_next_nodes")

        if self.valid_previous_nodes_policy == 'allow_none' and self.valid_previous_nodes:
            raise ValueError(f"allow_none valid_previous_nodes_policy cannot take a list of valid_previous_nodes")
        elif self.valid_previous_nodes_policy == 'custom' and not self.valid_previous_nodes:
            raise ValueError(f"custom valid_previous_nodes_policy policy requires a list of valid_previous_nodes")

        if self.valid_incoming_edges_policy == 'allow_none' and self.valid_incoming_edges:
            raise ValueError(f"allow_none valid_incoming_edges_policy cannot take a list of valid_incoming_edges")
        elif self.valid_incoming_edges_policy == 'custom' and not self.valid_incoming_edges:
            raise ValueError(f"custom valid_incoming_edges_policy policy requires a list of valid_incoming_edges")

        if self.valid_outgoing_edges_policy == 'allow_none' and self.valid_outgoing_edges:
            raise ValueError(f"allow_none valid_outgoing_edges_policy cannot take a list of valid_outgoing_edges")
        elif self.valid_outgoing_edges_policy == 'custom' and not self.valid_outgoing_edges:
            raise ValueError(f"custom valid_outgoing_edges_policy policy requires a list of valid_outgoing_edges")
