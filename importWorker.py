import gc
from PyQt5.QtCore import QVariant, QRegExp ,QTime, QDate, QDateTime
from qgis.PyQt.QtCore import pyqtSignal, Qt, QCoreApplication, QThread, pyqtSlot
from qgis.core import (
    QgsVectorLayer, QgsGeometry, QgsFeature, QgsProject,
    QgsFields, QgsField, Qgis, QgsWkbTypes
)
from decimal import Decimal
from datetime import datetime, date, time

class ImportWorker(QThread):
    """Import in a background thread to avoid blocking the main UI"""
    progress_updated = pyqtSignal(int)
    finished = pyqtSignal(object)
    error_occurred = pyqtSignal(str)


    def __init__(self, plugin, layer_name):
        super().__init__()
        self.plugin = plugin
        self.layer_name = layer_name
        self.bad_value_reports = 0
        self._bad_fields = set()


    @staticmethod
    def _parse_datetime(text):
        """按常见格式解析字符串，解析不了返回 None。"""
        if not text:
            return None
        text = text.strip()
        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%H:%M:%S.%f",
            "%H:%M:%S",
        ):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        return None


    @classmethod
    def convert_value(cls, qvariant_type, value):
        """把达梦驱动返回的原始值转成 QGIS 字段可接受的 Qt 值。

        关键点：NULL（None）必须原样返回 None，绝不能去访问 .year/.hour 等属性，
        否则会抛 'NoneType' object has no attribute 'year' 并中断整批导入。
        """
        if value is None:
            return None

        if qvariant_type in (QVariant.Int, getattr(QVariant, "LongLong", QVariant.Int)):
            if isinstance(value, Decimal):
                return int(value)
            if isinstance(value, str):
                text = value.strip()
                return int(text) if text else None
            return int(value)

        if qvariant_type == QVariant.Double:
            if isinstance(value, str):
                text = value.strip()
                return float(text) if text else None
            return float(value)

        if qvariant_type == QVariant.Time:
            if isinstance(value, datetime):
                return QTime(value.hour, value.minute, value.second,
                             value.microsecond // 1000)
            if isinstance(value, time):
                return QTime(value.hour, value.minute, value.second,
                             value.microsecond // 1000)
            dt = cls._parse_datetime(value) if isinstance(value, str) else None
            if dt is None:
                return None
            return QTime(dt.hour, dt.minute, dt.second, dt.microsecond // 1000)

        if qvariant_type == QVariant.Date:
            if isinstance(value, datetime):
                return QDate(value.year, value.month, value.day)
            if isinstance(value, date):
                return QDate(value.year, value.month, value.day)
            dt = cls._parse_datetime(value) if isinstance(value, str) else None
            return QDate(dt.year, dt.month, dt.day) if dt else None

        if qvariant_type == QVariant.DateTime:
            if isinstance(value, datetime):
                return QDateTime(
                    QDate(value.year, value.month, value.day),
                    QTime(value.hour, value.minute, value.second,
                          value.microsecond // 1000))
            if isinstance(value, date):
                return QDateTime(QDate(value.year, value.month, value.day),
                                 QTime(0, 0))
            dt = cls._parse_datetime(value) if isinstance(value, str) else None
            if dt is None:
                return None
            return QDateTime(
                QDate(dt.year, dt.month, dt.day),
                QTime(dt.hour, dt.minute, dt.second, dt.microsecond // 1000))

        if qvariant_type == QVariant.String:
            if isinstance(value, str):
                return value
            if isinstance(value, (bytes, bytearray)):
                try:
                    return value.decode("utf-8")
                except UnicodeDecodeError:
                    return value.hex()
            if isinstance(value, (Decimal, int, float)):
                return str(value)
            # 其它对象（例如误混进来的达梦几何对象）直接丢弃，
            # 否则一条坏值会让整批 addFeatures 全部被拒
            return None

        return value


    @staticmethod
    def _fit_geometry(geom, layer_wkb):
        """把几何类型对齐到图层类型；无法对齐时返回 None（该要素跳过）。"""
        flat_layer = QgsWkbTypes.flatType(layer_wkb)
        flat_geom = QgsWkbTypes.flatType(geom.wkbType())
        if flat_geom == flat_layer:
            return geom
        # 单类型 -> 多类型：图层是 Multi* 时自动提升
        if QgsWkbTypes.isMultiType(flat_layer) and not QgsWkbTypes.isMultiType(flat_geom):
            multi = geom.convertToMultiType()
            if multi is not None and not multi.isEmpty():
                return multi
            return None
        # 其余情况（多->单、点/线/面互转）不做强转，交由调用方跳过并计数
        return None


    @staticmethod
    def _provider_error(layer, provider):
        """读取 provider/layer 记录的错误原文（memory provider 用 pushError 记录原因）。"""
        parts = []
        for obj, label in ((provider, "provider"), (layer, "layer")):
            for getter in ("error", "lastError"):
                try:
                    res = getattr(obj, getter)()
                except Exception:
                    continue
                try:
                    text = res.message() if hasattr(res, "message") else str(res)
                except Exception:
                    continue
                if text:
                    parts.append(f"{label}.{getter}={text}")
        return " ; ".join(parts)


    @staticmethod
    def _diagnose_one(layer, uri, feat, provider_err=""):
        """把"第一条失败要素"的关键信息一次性打出来，便于直接定位。"""
        geom = feat.geometry()
        if geom is not None and not geom.isNull():
            feat_wkb = QgsWkbTypes.displayString(geom.wkbType())
            wkt_head = geom.asWkt(3)[:60]
        else:
            feat_wkb = "NULL"
            wkt_head = ""
        attr_types = [type(v).__name__ for v in feat.attributes()]
        return (
            f"add features fail（整批被拒，已逐条定位到失败要素）| uri={uri} | "
            f"layerWkb={QgsWkbTypes.displayString(layer.wkbType())} | "
            f"featureWkb={feat_wkb} | layerFields={layer.fields().count()} | "
            f"featureAttrs={len(feat.attributes())} | attrTypes={attr_types} | "
            f"wkt={wkt_head} | providerError={provider_err}")


    def _build_fields(self, fields_meta):
        """按达梦字段元数据构造 QgsFields：字段名去重、空名兜底。

        重复字段名会让 provider.addAttributes() 失败，图层字段数少于数据列数，
        进而导致每条要素属性越界、整批 addFeatures 被拒。
        """
        qgis_fields = QgsFields()
        used_names = set()
        for field_name, dm_type in fields_meta:
            qgis_type = self.plugin.dm_type_to_qgis(dm_type)
            base = (str(field_name) or "").strip() or "field"
            name = base
            idx = 1
            while name.lower() in used_names:
                idx += 1
                name = f"{base}_{idx}"
            used_names.add(name.lower())
            qgis_fields.append(QgsField(name, qgis_type))
        return qgis_fields


    def run(self):
        """Thread execution entry"""
        try:
            # 1. Query all fields of the table (excluding spatial columns)
            fields_meta = self.plugin.get_table_fields()
            if not fields_meta:
                #iface.messageBar().pushWarning("Warning", self.tr("Could not find valid field"))
                self.error_occurred.emit(f"Could not find valid field")
                return

            # 2. Dynamically create fields in QGIS
            qgis_fields = self._build_fields(fields_meta)

            if self.isInterruptionRequested():
                return

            #for field in qgis_fields:
                #field_type = QVariant.typeToName(field.type())
                #iface.messageBar().pushMessage("field type", f"{field.name()}: {field_type}")

            # 3. 先取一批数据，用"数据中第一条有效几何"的真实类型建图层。
            #    之前直接用 typeBox 的文本建图层，一旦数据是 MULTIPOINT / 3D，
            #    addFeatures 会把整批全部拒掉（failed number = 批大小）。
            batch_size = 1000
            self.plugin.get_limit_data(fields_meta)
            first_batch = self.plugin.cursor.fetchmany(batch_size) or []

            layer_wkb = None
            sample_wkt = None
            for row in first_batch:
                wkt_value = row[-1]
                geom = QgsGeometry.fromWkt(wkt_value) if wkt_value else None
                if geom is not None and not geom.isEmpty() and geom.isGeosValid():
                    layer_wkb = geom.wkbType()
                    sample_wkt = wkt_value
                    break

            if not first_batch:
                self.error_occurred.emit(
                    f"查询未返回任何数据（0 行）：srid={self.plugin.srid}，"
                    f"几何类型={self.plugin.type_name}。"
                    f"请核对这两个过滤条件是否与表定义一致"
                    f"（本表为 SRID=4326 / ST_POINT）")
                return

            layer_type = (QgsWkbTypes.displayString(layer_wkb)
                          if layer_wkb is not None else self.plugin.type_name)
            crs = f"EPSG:{self.plugin.srid}"
            uri = f"{layer_type}?crs={crs}"
            layer = QgsVectorLayer(uri, self.layer_name, "memory")
            if not layer.isValid():
                self.error_occurred.emit(
                    f"图层创建失败：uri={uri}, srid={self.plugin.srid}, "
                    f"sampleWkt={str(sample_wkt)[:80]}")
                return

            provider = layer.dataProvider()
            if not provider.addAttributes(qgis_fields):
                self.error_occurred.emit(
                    f"图层字段创建失败（字段名可能重复或非法）：{[f.name() for f in qgis_fields]}")
                return
            layer.updateFields()
            field_count = layer.fields().count()
            if field_count != len(fields_meta):
                self.error_occurred.emit(
                    f"字段数量不一致：查询到 {len(fields_meta)} 个，图层只建了 {field_count} 个")
                return

            

            # 4. 分批写入要素（属性 + 几何），动态进度
            total = self.plugin.get_total_size()
            total_size = int(total[0]) if total and total[0] is not None else 0
            offset = 0
            import_size = 0
            skipped = 0

            while True:
                batch_data = first_batch if offset == 0 else self.plugin.cursor.fetchmany(batch_size)
                first_batch = []
                if not batch_data:
                    break
                
                # Create feature in batches
                batch_features = []
                for i, row in enumerate(batch_data):
                    if self.isInterruptionRequested():
                        return

                    # 拆分属性值与 WKT（最后一列是 WKT）
                    attributes = row[:-1]
                    wkt_value = row[-1]
                    geom = QgsGeometry.fromWkt(wkt_value) if wkt_value else None
                    if geom is None or geom.isEmpty() or not geom.isGeosValid():
                        skipped += 1
                        continue

                    # 几何类型向图层类型对齐，尽量避免整批被拒
                    geom = self._fit_geometry(geom, layer.wkbType())
                    if geom is None:
                        skipped += 1
                        continue

                    if len(attributes) != field_count:
                        self.error_occurred.emit(
                            f"属性列数与图层字段数不一致：row={len(attributes)}, layer={field_count}")
                        return

                    # 关键：带字段构造要素，保证属性 schema 与图层一致
                    feat = QgsFeature(layer.fields())
                    feat.setGeometry(geom)
                    converted_attrs = []
                    attr_error = False
                    for attr_idx, attr_value in enumerate(attributes):
                        field = qgis_fields[attr_idx]
                        try:
                            converted_attrs.append(
                                self.convert_value(field.type(), attr_value))
                        except Exception as e:
                            self.error_occurred.emit(
                                f"Convert field [{field.name()}] failed, value={attr_value!r}: {e}")
                            attr_error = True
                            break
                    if attr_error:
                        # 跳过这一条要素，不中断整批导入
                        continue

                    # 逐值类型校验：类型和字段不符时只跳过该条并点名，
                    # 否则一条坏值会让整批 addFeatures 全部被拒
                    bad_value = None
                    layer_fields = layer.fields()
                    for attr_idx, value in enumerate(converted_attrs):
                        if value is None:
                            continue
                        fld = layer_fields.at(attr_idx)
                        fld_type = fld.type()
                        if fld_type in (QVariant.Int, getattr(QVariant, "LongLong", QVariant.Int)):
                            if not isinstance(value, int):
                                bad_value = (fld.name(), value)
                            elif fld_type == QVariant.Int and not (-2147483648 <= value <= 2147483647):
                                # 超过 32 位整数范围，provider 的 convertCompatible 会失败
                                bad_value = (fld.name(), value)
                        elif fld_type == QVariant.Double:
                            if not isinstance(value, float):
                                bad_value = (fld.name(), value)
                        elif fld_type == QVariant.String:
                            if not isinstance(value, str):
                                bad_value = (fld.name(), value)
                        elif fld_type in (QVariant.Date, QVariant.Time, QVariant.DateTime):
                            if not isinstance(value, (QDate, QTime, QDateTime)):
                                bad_value = (fld.name(), value)
                        if bad_value:
                            break
                    if bad_value:
                        # 同一条坏值会重复上千次，这里只报前 3 条，最后统一汇总
                        self.bad_value_reports += 1
                        self._bad_fields.add(bad_value[0])
                        if self.bad_value_reports <= 3:
                            self.error_occurred.emit(
                                f"属性值类型不符，已跳过该要素：字段={bad_value[0]}，"
                                f"值={bad_value[1]!r}")
                        skipped += 1
                        continue

                    feat.setAttributes(converted_attrs)  # The order of attributes is consistent with the order of fields
                    batch_features.append(feat)               
                if batch_features:
                    success, failed = provider.addFeatures(batch_features)
                    if success:
                        layer.updateExtents()
                        import_size += len(batch_features)
                        if total_size > 0:
                            progress = min(100, int(import_size / total_size * 100))
                            self.progress_updated.emit(progress)
                    else:
                        # 整批被拒时逐条重试：既能保住可写入的要素，又能定位第一条坏数据
                        first_bad = None
                        bad = 0
                        for one in batch_features:
                            if provider.addFeature(one):
                                import_size += 1
                            else:
                                bad += 1
                                if first_bad is None:
                                    first_bad = one
                        if total_size > 0:
                            progress = min(100, int(import_size / total_size * 100))
                            self.progress_updated.emit(progress)
                        if first_bad is not None:
                            self.error_occurred.emit(
                                self._diagnose_one(layer, uri, first_bad,
                                                   self._provider_error(layer, provider)))
                        elif bad:
                            self.error_occurred.emit(
                                f"add features fail, failed number:{bad} | uri={uri}")

                batch_data.clear()  # Clear batch_data
                del batch_data  # Delete the reference
                batch_features.clear()  # Clear batch_features
                del batch_features  # Delete the reference
                gc.collect()
                offset += batch_size

            if self.bad_value_reports:
                self.error_occurred.emit(
                    f"共 {self.bad_value_reports} 条要素因属性值类型不符被跳过，"
                    f"涉及字段：{sorted(self._bad_fields)}")

            self.progress_updated.emit(100)
            self.finished.emit(layer)

        except Exception as e:
            self.error_occurred.emit(str(e))
        