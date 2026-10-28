using System.ComponentModel;
using System.ComponentModel.DataAnnotations;

namespace ExampleKafka
{
    /// <summary>
    /// The message value. This class is the SOURCE OF TRUTH for the JSON Schema --
    /// NJsonSchema.JsonSchema.FromType&lt;Value&gt;() derives the schema from these
    /// properties and attributes at registration time (code-first). This is the
    /// opposite direction from Avro/Protobuf, where the schema file is the source of
    /// truth and this class would instead be generated FROM it.
    /// </summary>
    [Description("An order placed by a customer.")]
    public class Value
    {
        [Required]
        [Description("Unique order identifier.")]
        public string OrderId { get; set; } = "";

        [Description("Customer who placed the order.")]
        public string CustomerId { get; set; } = "";

        [Description("Order total.")]
        public double Total { get; set; } = 0;

        [Description("When the order was placed (ISO 8601).")]
        public string Timestamp { get; set; } = "";

        [Description("Optional metadata associated with the order.")]
        public string? Metadata { get; set; } = null;
    }
}
