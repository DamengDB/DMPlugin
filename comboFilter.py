
from PyQt5.QtCore import QObject, QEvent, QTimer

class PopupFilter(QObject):
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent

    def eventFilter(self, obj, event):
        if event.type() == QEvent.Show:
            popup = obj.window()
            if popup:
                pos = self.parent.mapToGlobal(self.parent.rect().bottomLeft())
                QTimer.singleShot(0, lambda: (popup.setMaximumHeight(200), popup.move(pos)))

        return False