import scala.Predef;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;

import java.util.HashMap;

public class Utils {
  static Map<String, Object> immutableScalaMap(java.util.Map<String, Object> map) {
    return JavaConverters.mapAsScalaMapConverter(map).asScala().toMap(Predef.$conforms());
  }

  static Map<String, Object> emptyImmutableScalaMap() {
    return Utils.immutableScalaMap(new HashMap<>());
  }
}
