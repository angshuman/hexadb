using System;
using System.Collections.Generic;
using System.Linq;

namespace Hexastore.Client
{
    public class DataGenerator
    {
        private readonly int level;
        private readonly int factor;
        private readonly string relationshipName;
        private readonly int numTwinProperties;
        private readonly int numEdgeProperties;
        private readonly bool generateReverseEdge;
        private readonly Random rand;
        private int nodeCount = 1;
        private int edgeCount = 0;

        public DataGenerator(int level, int factor, string relationshipName, int numTwinProperties, int numEdgeProperties, bool generateReverseEdge)
        {
            this.level = level;
            this.factor = factor;
            this.relationshipName = relationshipName;
            this.numTwinProperties = numTwinProperties;
            this.numEdgeProperties = numEdgeProperties;
            this.generateReverseEdge = generateReverseEdge;
            rand = new Random();
        }

        public IEnumerable<object> Generate()
        {
            var root = new List<Node> { GetNode("0", "twin", 0) };
            yield return root.First();
            var rest = GenerateLevel(root, 1);
            foreach (var item in rest) {
                yield return item;
            }
        }

        private IEnumerable<object> GenerateLevel(List<Node> previousNodes, int currentLevel)
        {
            if (currentLevel == level) {
                yield break;
            }

            var levelNodes = new List<Node>();

            foreach (var previousNode in previousNodes) {
                for (var count = 0; count < factor; count++) {
                    var newNode = GetNode(id: nodeCount++.ToString(), label: "twin", level: currentLevel);
                    levelNodes.Add(newNode);
                    yield return newNode;

                    foreach (var edge in GetEdge(edgeCount++.ToString(), relationshipName, previousNode.Id, newNode.Id, generateReverseEdge)) {
                        yield return edge;
                    }
                }
            }

            foreach (var item in GenerateLevel(levelNodes, currentLevel + 1)) {
                yield return item;
            }
        }

        private Node GetNode(string id, string label, int level)
        {
            var newNode = new Node {
                Id = id,
                Label = label,
            };
            newNode.PartitionId = newNode.Id;
            newNode.Properties.Add("temperature", rand.Next(50, 100));
            newNode.Properties.Add("humidity", rand.Next(50, 100));
            newNode.Properties.Add("pressure", rand.Next(50, 100));
            newNode.Properties.Add("level", level);

            for (var i = 0; i < numTwinProperties; i++) {
                var propName = $"prop{i.ToString("D3")}";
                newNode.Properties[propName] = rand.Next(0, 100);
            }

            return newNode;
        }

        private IEnumerable<Edge> GetEdge(string id, string label, string from, string to, bool generateReverse)
        {
            var properties = new Dictionary<string, object> { { "length", rand.Next(10) } };
            for (var i = 0; i < numEdgeProperties; i++) {
                var propName = $"prop{i.ToString("D3")}";
                properties[propName] = rand.Next(0, 100);
            }

            yield return new Edge {
                Id = id,
                Label = label,
                EdgeType = "Outgoing",
                FromId = from,
                ToId = to,
                Properties = properties,
            };

            if (generateReverse) {
                yield return new Edge {
                    Id = $"re:{id}",
                    Label = label,
                    EdgeType = "Reverse",
                    FromId = to,
                    ToId = from,
                    Properties = properties,
                };
            }
        }
    }

    public class Node
    {
        public string Id { get; set; }

        public string Label { get; set; }

        public string PartitionId { get; set; }

        public Dictionary<string, object> Properties { get; set; } = new Dictionary<string, object>();
    }

    public class Edge
    {
        public string Id { get; set; }

        public string Label { get; set; }

        public string EdgeType { get; set; }

        public string FromId { get; set; }

        public string ToId { get; set; }

        public Dictionary<string, object> Properties { get; set; } = new Dictionary<string, object>();
    }
}

