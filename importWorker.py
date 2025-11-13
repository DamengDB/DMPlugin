import gc
from PyQt5.QtCore import QVariant, QRegExp ,QTime, QDate, QDateTime
from qgis.PyQt.QtCore import pyqtSignal, Qt, QCoreApplication, QThread, pyqtSlot
from qgis.core import (
    QgsVectorLayer, QgsGeometry, QgsFeature, QgsProject,
    QgsFields, QgsField, Qgis
)
from decimal import Decimal
from datetime import datetime, date, time

class ImportWorker(QThread):
    """后台导入线程，避免堵塞主UI"""
    progress_updated = pyqtSignal(int)
    finished = pyqtSignal(object)
    error_occurred = pyqtSignal(str)


    def __init__(self, plugin, layer_name):
        super().__init__()
        self.plugin = plugin
        self.layer_name = layer_name


    def run(self):
        """线程执行入口"""
        try:
            # 1. 获取表字段元数据(非空间数据列字段)
            fields_meta = self.plugin.get_table_fields()
            if not fields_meta:
                #iface.messageBar().pushWarning("Warning", self.tr("Could not find valid field"))
                self.error_occurred.emit(f"Could not find valid field")
                return

            # 2. 动态创建QGIS字段
            qgis_fields = QgsFields()
            for field_name, dm_type in fields_meta:
                qgis_type = self.plugin.dm_type_to_qgis(dm_type)
                qgis_fields.append(QgsField(field_name, qgis_type))

            if self.isInterruptionRequested():
                return

            # 打印字段类型，用于调试
            #for field in qgis_fields:
                #field_type = QVariant.typeToName(field.type())
                #iface.messageBar().pushMessage("field type", f"{field.name()}: {field_type}")

            # 3. 创建内存图层
            layer_type = self.plugin.type_name
            crs = f"EPSG:{self.plugin.srid}"  # 可根据实际数据坐标系调整
            uri = f"{layer_type}?crs={crs}"
            layer = QgsVectorLayer(uri, self.layer_name, "memory")
            layer.dataProvider().addAttributes(qgis_fields)
            layer.updateFields()
            

            # 4. 筛选条件、添加要素（属性+几何） 动态进度
            total_size = self.plugin.get_total_size()[0]
            offset = 0
            batch_size = 1000
            import_size = 0
            self.plugin.get_limit_data(fields_meta)

            while offset < total_size:
                # 分批获取数据
                batch_data = self.plugin.cursor.fetchmany(batch_size)
                if not batch_data:
                    break
                
                # 分批创建要素
                batch_features = []
                for i, row in enumerate(batch_data):
                    if self.isInterruptionRequested():
                        return

                    # 拆分属性值和WKT（最后一列是WKT）
                    attributes = row[:-1]
                    wkt_value = row[-1]
                    # 创建几何
                    geom = QgsGeometry.fromWkt(wkt_value)
                    if geom is None:
                        self.error_occurred.emit(f"WKT is invalid：{wkt_value}")
                        continue
                    
                    # 创建要素并绑定属性和几何
                    feat = QgsFeature()
                    feat.setGeometry(geom)
                    converted_attrs = []
                    for attr_idx, attr_value in enumerate(attributes):
                        field = qgis_fields[attr_idx]
                        if field.type() == QVariant.Int:
                            if isinstance(attr_value, Decimal):
                                try:
                                    attr_value = int(float(attr_value))
                                except:
                                    self.error_occurred.emit(f"Decimal convert to Int failed：{attr_value}")
                                    converted_attrs = None
                                    break
                            elif isinstance(attr_value, str):
                                try:
                                    attr_value = int(attr_value.strip())
                                except:
                                    self.error_occurred.emit(f"Str convert to Int failed：{attr_value}")
                                    converted_attrs = None
                                    break
                        elif field.type() == QVariant.Time:
                            qtime = QTime(attr_value.hour, attr_value.minute, attr_value.second, attr_value.microsecond // 1000)
                            attr_value = qtime
                            if isinstance(attr_value, str):
                                try:
                                    attr_value = datetime.strptime(attr_value, "%H:%M:%S").time()
                                except:
                                    iface.messageBar().pushWarning("Warning", f"str convert to time failed：{attr_value}")
                                    converted_attrs = None
                                    break
                        elif field.type() == QVariant.Date:
                            qdate = QDate(attr_value.year, attr_value.month, attr_value.day)
                            attr_value = qdate
                        elif field.type() == QVariant.DateTime:
                            qdatetime = QDateTime(attr_value.year, attr_value.month, attr_value.day, attr_value.hour, attr_value.minute, attr_value.second, attr_value.microsecond // 1000)
                            attr_value = qdatetime
                        converted_attrs.append(attr_value)
                    if converted_attrs is not None:
                        feat.setAttributes(converted_attrs)  # 属性顺序与字段顺序一致
                    batch_features.append(feat)

                if batch_features:
                    success, failed = layer.dataProvider().addFeatures(batch_features)
                    if success:
                        layer.updateExtents()
                        import_size += len(batch_features)
                        # 更新进度
                        progress = int(import_size / total_size * 100) # 动态进度
                        self.progress_updated.emit(progress)
                    else:
                        self.error_occurred.emit(f"add features fail, failed number:{len(failed)}")

                batch_data.clear()  # 清空列表
                del batch_data  # 删除引用
                batch_features.clear()  # 清空列表
                del batch_features  # 删除引用
                gc.collect()
                offset += batch_size

            self.progress_updated.emit(100)
            self.finished.emit(layer)

        except Exception as e:
            self.error_occurred.emit(str(e))
        