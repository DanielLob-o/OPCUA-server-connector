from client import *
from opcua import ua
import os


"""
   The possible types of a variant.

   :ivar Null:
   :ivar Boolean:
   :ivar SByte:
   :ivar Byte:
   :ivar Int16:
   :ivar UInt16:
   :ivar Int32:
   :ivar UInt32:
   :ivar Int64:
   :ivar UInt64:
   :ivar Float:
   :ivar Double:
   :ivar String:
   :ivar DateTime:
   :ivar Guid:
   :ivar ByteString:
   :ivar XmlElement:
   :ivar NodeId:
   :ivar ExpandedNodeId:
   :ivar StatusCode:
   :ivar QualifiedName:
   :ivar LocalizedText:
   :ivar ExtensionObject:
   :ivar DataValue:
   :ivar Variant:
   :ivar DiagnosticInfo:
   """

client = Client(os.getenv('OPC_UA_IP', ''))

#Lee una variable a partir del NodeID
def leervariable(nodo):
    try:
        client.connect()
        var = client.get_node(nodo).get_value()
        client.disconnect()
        return var
    except Exception as e:
        return f"Cannot read node. {e}", 400

#Lee las variables del seguidor
def leer_var_seguidor(nodo):
    values = []
    for k in client.get_node(nodo).get_children():
        values.append(client.get_node(k).get_value())
    return values

#Lee un array a partir del NodeID
def leerarray(nodo):
    values = []
    for k in client.get_node(nodo).get_children():
        values.append(client.get_node(k).get_value())
    return values

#Escribe al nodo asignado el valor decidido
def escribirval(nodos):
    try:
        client.connect()
        for k, v in nodos.items():
                var = client.get_node(k)
                if str(var.get_data_type_as_variant_type()) == 'VariantType.Int16':
                    dv = ua.DataValue(ua.Variant(v, ua.VariantType.Int16))
                elif str(var.get_data_type_as_variant_type()) == 'VariantType.Boolean':
                    dv = ua.DataValue(ua.Variant(v, ua.VariantType.Boolean))
                elif str(var.get_data_type_as_variant_type()) == 'VariantType.Float':
                    dv = ua.DataValue(ua.Variant(v, ua.VariantType.Float))
                elif str(var.get_data_type_as_variant_type()) == 'VariantType.Double':
                    dv = ua.DataValue(ua.Variant(v, ua.VariantType.Double))
                elif str(var.get_data_type_as_variant_type()) == 'VariantType.String':
                    dv = ua.DataValue(ua.Variant(v, ua.VariantType.String))
                elif str(var.get_data_type_as_variant_type()) == 'VariantType.Int32':
                    dv = ua.DataValue(ua.Variant(v, ua.VariantType.Int32))
                elif str(var.get_data_type_as_variant_type()) == 'VariantType.UInt16':
                    dv = ua.DataValue(ua.Variant(v, ua.VariantType.UInt16))
                elif str(var.get_data_type_as_variant_type()) == 'VariantType.UInt32':
                    dv = ua.DataValue(ua.Variant(v, ua.VariantType.UInt32))
                elif str(var.get_data_type_as_variant_type()) == 'VariantType.DateTime':
                    dv = ua.DataValue(ua.Variant(v, ua.VariantType.DateTime))
                var.set_value(dv)
        client.disconnect()
    except Exception as e:
        raise
