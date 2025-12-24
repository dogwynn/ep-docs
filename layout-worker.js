// ForceAtlas2 Web Worker - Self-contained implementation
// No external dependencies

self.onmessage = function(e) {
    const { nodes, edges, settings } = e.data;

    const totalIterations = settings.iterations || 100;
    const gravity = settings.gravity || 1;
    const scalingRatio = settings.scalingRatio || 10;

    // Build node map with positions and velocities
    const nodeMap = {};
    nodes.forEach(node => {
        nodeMap[node.id] = {
            x: node.x,
            y: node.y,
            dx: 0,
            dy: 0,
            mass: node.size || 1
        };
    });

    // Build adjacency for faster lookup
    const adjacency = {};
    nodes.forEach(n => adjacency[n.id] = []);
    edges.forEach(edge => {
        if (nodeMap[edge.source] && nodeMap[edge.target]) {
            adjacency[edge.source].push({ target: edge.target, weight: edge.weight || 1 });
            adjacency[edge.target].push({ target: edge.source, weight: edge.weight || 1 });
        }
    });

    const nodeIds = Object.keys(nodeMap);
    const nodeCount = nodeIds.length;

    // Barnes-Hut tree node
    function QuadTree(x, y, width, height) {
        this.x = x;
        this.y = y;
        this.width = width;
        this.height = height;
        this.mass = 0;
        this.centerX = 0;
        this.centerY = 0;
        this.nodes = [];
        this.children = null;
    }

    QuadTree.prototype.insert = function(node) {
        if (this.children) {
            const midX = this.x + this.width / 2;
            const midY = this.y + this.height / 2;
            const index = (node.x < midX ? 0 : 1) + (node.y < midY ? 0 : 2);
            this.children[index].insert(node);
        } else {
            this.nodes.push(node);
            if (this.nodes.length > 4 && this.width > 1) {
                this.subdivide();
            }
        }
        this.mass += node.mass;
        this.centerX = (this.centerX * (this.mass - node.mass) + node.x * node.mass) / this.mass;
        this.centerY = (this.centerY * (this.mass - node.mass) + node.y * node.mass) / this.mass;
    };

    QuadTree.prototype.subdivide = function() {
        const hw = this.width / 2;
        const hh = this.height / 2;
        this.children = [
            new QuadTree(this.x, this.y, hw, hh),
            new QuadTree(this.x + hw, this.y, hw, hh),
            new QuadTree(this.x, this.y + hh, hw, hh),
            new QuadTree(this.x + hw, this.y + hh, hw, hh)
        ];
        const nodes = this.nodes;
        this.nodes = [];
        nodes.forEach(n => this.insert(n));
    };

    QuadTree.prototype.calculateForce = function(node, theta, repulsion) {
        if (this.mass === 0) return { fx: 0, fy: 0 };

        const dx = this.centerX - node.x;
        const dy = this.centerY - node.y;
        const dist = Math.sqrt(dx * dx + dy * dy) || 0.01;

        if (this.children && this.width / dist > theta) {
            let fx = 0, fy = 0;
            for (let i = 0; i < 4; i++) {
                const f = this.children[i].calculateForce(node, theta, repulsion);
                fx += f.fx;
                fy += f.fy;
            }
            return { fx, fy };
        } else {
            if (this.nodes.length === 1 && this.nodes[0] === node) {
                return { fx: 0, fy: 0 };
            }
            const force = -repulsion * node.mass * this.mass / (dist * dist);
            return {
                fx: force * dx / dist,
                fy: force * dy / dist
            };
        }
    };

    // Run iterations
    for (let iter = 0; iter < totalIterations; iter++) {
        // Calculate bounds for quad tree
        let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
        nodeIds.forEach(id => {
            const n = nodeMap[id];
            minX = Math.min(minX, n.x);
            minY = Math.min(minY, n.y);
            maxX = Math.max(maxX, n.x);
            maxY = Math.max(maxY, n.y);
        });

        const padding = 10;
        const width = Math.max(maxX - minX + padding * 2, 100);
        const height = Math.max(maxY - minY + padding * 2, 100);

        // Build quad tree
        const tree = new QuadTree(minX - padding, minY - padding, width, height);
        nodeIds.forEach(id => tree.insert(nodeMap[id]));

        // Reset forces
        nodeIds.forEach(id => {
            nodeMap[id].dx = 0;
            nodeMap[id].dy = 0;
        });

        // Repulsion (Barnes-Hut)
        const repulsion = scalingRatio * 100;
        nodeIds.forEach(id => {
            const node = nodeMap[id];
            const f = tree.calculateForce(node, 0.5, repulsion);
            node.dx += f.fx;
            node.dy += f.fy;
        });

        // Attraction (edges)
        edges.forEach(edge => {
            const source = nodeMap[edge.source];
            const target = nodeMap[edge.target];
            if (!source || !target) return;

            const dx = target.x - source.x;
            const dy = target.y - source.y;
            const dist = Math.sqrt(dx * dx + dy * dy) || 0.01;

            const force = dist / 100;
            const fx = force * dx / dist;
            const fy = force * dy / dist;

            source.dx += fx;
            source.dy += fy;
            target.dx -= fx;
            target.dy -= fy;
        });

        // Gravity
        nodeIds.forEach(id => {
            const node = nodeMap[id];
            const dist = Math.sqrt(node.x * node.x + node.y * node.y) || 0.01;
            node.dx -= gravity * node.x / dist * node.mass;
            node.dy -= gravity * node.y / dist * node.mass;
        });

        // Apply forces with speed limit
        const speed = 1 / (1 + Math.sqrt(iter));
        const maxDisplacement = 10;

        nodeIds.forEach(id => {
            const node = nodeMap[id];
            const displacement = Math.sqrt(node.dx * node.dx + node.dy * node.dy);
            if (displacement > 0) {
                const limitedDisp = Math.min(displacement, maxDisplacement) * speed;
                node.x += node.dx / displacement * limitedDisp;
                node.y += node.dy / displacement * limitedDisp;
            }
        });

        // Report progress every 10 iterations
        if (iter % 10 === 0 || iter === totalIterations - 1) {
            self.postMessage({
                type: 'progress',
                progress: Math.round((iter + 1) / totalIterations * 100)
            });
        }
    }

    // Return final positions
    const positions = {};
    nodeIds.forEach(id => {
        positions[id] = { x: nodeMap[id].x, y: nodeMap[id].y };
    });

    self.postMessage({
        type: 'complete',
        positions: positions
    });
};
