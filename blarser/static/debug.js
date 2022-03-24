let tooltip;

async function addEntityView(entityType, entityId) {
    // fetch data and render
    const resp = await fetch(`/debug/${entityType}/${entityId}`);
    const data = await resp.json();
    const dag = d3.dagStratify()
        // d3-dag can't handle integer ids, so these funcs stringify them
        .id(datum => datum.id + "")
        .parentIds(datum => datum.parentIds.map(id => id + ""))
        (data);
    const nodeRadius = 20;
    const layout = d3
        .sugiyama() // base layout
        .decross(d3.decrossTwoLayer()) // minimize number of crossings
        .nodeSize((node) => [(node ? 15.6 : 0.25) * nodeRadius, 3 * nodeRadius]); // set node size instead of constraining to fit
    const {width, height} = layout(dag);

    // --------------------------------
    // This code only handles rendering
    // --------------------------------
    const svgSelection = d3.select("#main").append("svg");
    svgSelection
        .attr("id", `${entityType}:${entityId}`)
        .attr("viewBox", [0, 0, width, height].join(" "))
        .attr("width", width);

    // How to draw edges
    const line = d3
        .line()
        .curve(d3.curveCatmullRom)
        .x((d) => d.x)
        .y((d) => d.y);

    // Plot edges
    svgSelection
        .append("g")
        .selectAll("path")
        .data(dag.links())
        .enter()
        .append("path")
        .attr("d", ({points}) => line(points))
        .attr("fill", "none")
        .attr("stroke-width", 3)
        .attr("stroke", "black");

    // Select nodes
    const nodes = svgSelection
        .append("g")
        .selectAll("g")
        .data(dag.descendants())
        .enter()
        .append("g")
        .attr("class", "version")
        .attr("transform", ({x, y}) => `translate(${x}, ${y})`)
        .attr("title", ({data}) => data.startTime)
        .attr("data-bs-content", ({data}) => (
            (data.terminated ? `<p>Terminated: ${data.terminated}</p>\n` : "") +
            (data.observations.map(obs => `<p>Observed by: ${obs}</p>`).join("\n") +
            `<pre>${JSON.stringify(data.entity, null, 4)}</pre>`)
        ));

    // Plot node circles
    nodes
        .append("ellipse")
        .attr("rx", nodeRadius * 5)
        .attr("ry", nodeRadius)
        .attr("fill", (n) => n.data.terminated ? "red" : (n.data.observations.length > 0 ? "green" : "blue"));

    // Add text to nodes
    nodes
        .append("text")
        .text(({ data }) => data.startTime)
        .attr("font-weight", "bold")
        .attr("font-family", "sans-serif")
        .attr("text-anchor", "middle")
        .attr("alignment-baseline", "middle")
        .attr("fill", "white");
}

async function removeEntityView(entityType, entityId) {
    document.getElementById(`${entityType}:${entityId}`).remove()
}

document.addEventListener("DOMContentLoaded", function (event) {
    const main = document.getElementById("main");
    new bootstrap.Popover(main, {
        container: main,
        selector: '.version',
        placement: 'right',
        html: true,
        trigger: 'hover',
        boundary: main,
    })

    const entityList = document.getElementById("entity-list");
    entityList.addEventListener("click", event => {
        const li = event.target.closest('li'); // (1)
        if (!li) return; // (2)
        if (!entityList.contains(li)) return; // (3)
        // Clicking a label causes a click event on the corresponding input. Clicking the
        // input causes an event only in that input. Only respond to clicks that originate
        // on the input so there's only one event triggered.
        if (event.target.tagName !== "INPUT") return;

        const checked = event.target.checked;
        console.log("Checked entity id", li.dataset.entityId, checked);

        if (checked) {
            addEntityView(li.dataset.entityType, li.dataset.entityId);
        } else {
            removeEntityView(li.dataset.entityType, li.dataset.entityId);
        }
    });
});
