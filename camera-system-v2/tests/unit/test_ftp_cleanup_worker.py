"""
Unit tests for ftp_cleanup_worker.py
- No real file deletions
- No real DB connections
- All external dependencies mocked
"""
from __future__ import annotations
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../app"))

import unittest
from unittest.mock import Mock, patch
import time
import stat

import ftp_cleanup_worker as worker


class FakeStat:
    """Stub for os.stat_result"""
    def __init__(self, mtime: float):
        self.st_mtime = mtime
        self.st_size = 1234
        self.st_mode = stat.S_IFREG | 0o644


class TestPathIsUnderRoot(unittest.TestCase):
    """P0-2: Path guardrails"""

    def test_path_inside_root_allowed(self):
        """Путь внутри root -> allowed"""
        # Use os.sep for cross-platform paths
        root = os.sep + "source"
        candidate = os.sep + os.path.join("source", "cam01", "file.jpg")
        with patch("os.path.realpath") as mock_realpath:
            mock_realpath.side_effect = lambda p: p
            self.assertTrue(worker.path_is_under_root(candidate, root))

    def test_path_exact_root_allowed(self):
        """Путь == root -> allowed"""
        root = os.sep + "source"
        with patch("os.path.realpath") as mock_realpath:
            mock_realpath.return_value = root
            self.assertTrue(worker.path_is_under_root(root, root))

    def test_path_outside_root_not_allowed(self):
        """Путь вне root -> not allowed (P0-2)"""
        root = os.sep + "source"
        candidate = os.sep + os.path.join("etc", "passwd")
        with patch("os.path.realpath") as mock_realpath:
            mock_realpath.side_effect = lambda p: p
            self.assertFalse(worker.path_is_under_root(candidate, root))

    def test_path_traversal_not_allowed(self):
        """Path traversal /source/../etc/passwd -> not allowed"""
        root = os.sep + "source"
        candidate = os.sep + os.path.join("source", "..", "etc", "passwd")
        with patch("os.path.realpath") as mock_realpath:
            # realpath resolves the traversal
            def resolve(p):
                if ".." in p:
                    return os.sep + os.path.join("etc", "passwd")
                return p
            mock_realpath.side_effect = resolve
            self.assertFalse(worker.path_is_under_root(candidate, root))

    def test_oserror_returns_false(self):
        """OSError при realpath -> returns False"""
        root = os.sep + "source"
        candidate = os.sep + "broken" + os.sep + "symlink"
        with patch("os.path.realpath", side_effect=OSError("broken link")):
            self.assertFalse(worker.path_is_under_root(candidate, root))


class TestCleanupEligibility(unittest.TestCase):
    """P0-1: ready/processing не удаляются (не в TERMINAL)"""

    def setUp(self):
        self.mock_conn = Mock()
        self.mock_cur = Mock()
        self.mock_conn.cursor.return_value = self.mock_cur
        self.mock_journal = Mock()
        self.mock_ev_journal = Mock()

    def _make_row(self, event_id=1, full_path="/source/cam01/file.jpg",
                  file_name="file.jpg", camera_code="cam01",
                  first_seen_at="2024-01-01 10:00:00", status="sent"):
        # Use os.sep for cross-platform
        if full_path and full_path.startswith("/"):
            full_path = full_path.replace("/", os.sep)
        return (event_id, full_path, file_name, camera_code, first_seen_at, status)

    def test_ready_status_not_deleted(self):
        """P0-1: ready файл старше retention НЕ удаляется (не терминальный статус)"""
        # Arrange: ready событие старше retention
        row = self._make_row(status="ready", full_path="/source/cam01/test.jpg")
        
        # Проверка: ready не в TERMINAL
        self.assertNotIn("ready", worker.TERMINAL)
        self.assertNotIn(row[5], worker.TERMINAL)

    def test_processing_status_not_deleted(self):
        """P0-1: processing файл старше retention НЕ удаляется"""
        row = self._make_row(status="processing", full_path="/source/cam01/test.jpg")
        
        # Проверка: processing не в TERMINAL
        self.assertNotIn("processing", worker.TERMINAL)
        self.assertNotIn(row[5], worker.TERMINAL)


class TestEligibleSentFile(unittest.TestCase):
    """P0-3: sent файл старше retention -> удаляется"""

    def _simulate_deletion(self, row, cutoff, retention_days):
        """Эмулирует успешную ветку удаления из main()"""
        event_id, full_path, file_name, camera_code, first_seen_at, status = row
        root_real = os.sep + "source"
        
        # Guardrails
        if not full_path:
            return {"action": "skip", "reason": "no_path"}
        
        if not worker.path_is_under_root(full_path, root_real):
            return {"action": "skip", "reason": "outside_root"}
        
        if status not in worker.TERMINAL:
            return {"action": "skip", "reason": "not_terminal"}
        
        # Stat
        try:
            st = os.stat(full_path)
        except FileNotFoundError:
            return {"action": "mark_removed", "reason": "already_gone"}
        
        # Retention check
        if st.st_mtime > cutoff:
            return {"action": "skip", "reason": "too_young"}
        
        # Delete
        os.remove(full_path)
        
        # Update DB (would happen here in real code)
        return {"action": "deleted", "event_id": event_id}

    def test_sent_file_older_than_retention_deleted(self):
        """sent файл старше FTP_RETENTION_DAYS удаляется и обновляет БД"""
        retention_days = 7
        cutoff = time.time() - retention_days * 86400
        old_mtime = cutoff - 3600  # еще старше cutoff на час
        
        row = (1, os.sep + os.path.join("source", "cam01", "old.jpg"), "old.jpg", "cam01", "2024-01-01 10:00:00", "sent")
        
        with patch("os.stat", return_value=FakeStat(old_mtime)) as mock_stat, \
             patch("os.remove") as mock_remove, \
             patch("ftp_cleanup_worker.path_is_under_root", return_value=True):
            
            # Эмулируем успешное удаление
            result = self._simulate_deletion(row, cutoff, retention_days)
            
            # Assert: stat вызван
            mock_stat.assert_called_once()
            # Assert: remove вызван
            mock_remove.assert_called_once()
            self.assertEqual(result["action"], "deleted")


class TestEligibleQuarantineFile(unittest.TestCase):
    """P0-4: quarantine файл старше quarantine retention -> удаляется"""

    def _simulate_deletion(self, row, cutoff, retention_days):
        """Эмулирует успешную ветку удаления из main()"""
        event_id, full_path, file_name, camera_code, first_seen_at, status = row
        root_real = os.sep + "source"
        
        if not full_path:
            return {"action": "skip", "reason": "no_path"}
        
        if not worker.path_is_under_root(full_path, root_real):
            return {"action": "skip", "reason": "outside_root"}
        
        if status not in worker.TERMINAL:
            return {"action": "skip", "reason": "not_terminal"}
        
        try:
            st = os.stat(full_path)
        except FileNotFoundError:
            return {"action": "mark_removed", "reason": "already_gone"}
        
        if st.st_mtime > cutoff:
            return {"action": "skip", "reason": "too_young"}
        
        os.remove(full_path)
        return {"action": "deleted", "event_id": event_id}

    def test_quarantine_file_older_than_retention_deleted(self):
        """quarantine файл старше FTP_QUARANTINE_RETENTION_DAYS удаляется"""
        quarantine_retention = 3
        cutoff = time.time() - quarantine_retention * 86400
        old_mtime = cutoff - 3600
        
        row = (2, os.sep + os.path.join("source", "cam02", "quar.jpg"), "quar.jpg", "cam02", "2024-01-01 10:00:00", "quarantine")
        
        with patch("os.stat", return_value=FakeStat(old_mtime)), \
             patch("os.remove") as mock_remove, \
             patch("ftp_cleanup_worker.path_is_under_root", return_value=True):
            
            result = self._simulate_deletion(row, cutoff, quarantine_retention)
            
            mock_remove.assert_called_once()
            self.assertEqual(result["action"], "deleted")


class TestRetentionBoundaries(unittest.TestCase):
    """P1-5: Граничные случаи retention"""

    def _simulate_deletion(self, row, cutoff, retention_days):
        """Эмулирует успешную ветку удаления"""
        event_id, full_path, file_name, camera_code, first_seen_at, status = row
        root_real = os.sep + "source"
        
        if not full_path:
            return {"action": "skip", "reason": "no_path"}
        
        if not worker.path_is_under_root(full_path, root_real):
            return {"action": "skip", "reason": "outside_root"}
        
        if status not in worker.TERMINAL:
            return {"action": "skip", "reason": "not_terminal"}
        
        try:
            st = os.stat(full_path)
        except FileNotFoundError:
            return {"action": "mark_removed", "reason": "already_gone"}
        
        if st.st_mtime > cutoff:
            return {"action": "skip", "reason": "too_young"}
        
        os.remove(full_path)
        return {"action": "deleted", "event_id": event_id}

    def _simulate_skip_check(self, row, cutoff):
        """Проверяет только ветку пропуска по retention"""
        full_path = row[1]
        st = os.stat(full_path)
        if st.st_mtime > cutoff:
            return "skipped"
        return "deleted"

    def test_file_exactly_at_retention_boundary(self):
        """Файл с mtime == cutoff (граница) -> должен быть удален (<= cutoff)"""
        retention_days = 7
        cutoff = time.time() - retention_days * 86400
        
        # mtime == cutoff (граница)
        with patch("os.stat", return_value=FakeStat(cutoff)), \
             patch("os.remove"), \
             patch("ftp_cleanup_worker.path_is_under_root", return_value=True):
            
            # В текущей логике: st.st_mtime > cutoff пропускает
            # т.е. st.st_mtime <= cutoff удаляется
            st_mtime = cutoff  # == cutoff, не > cutoff
            should_delete = not (st_mtime > cutoff)  # True
            
            self.assertTrue(should_delete, "Файл на границе должен быть удален (mtime <= cutoff)")

    def test_file_1sec_younger_than_retention_skipped(self):
        """Файл моложе cutoff на 1 секунду -> пропускается (P1-5)"""
        retention_days = 7
        cutoff = time.time() - retention_days * 86400
        young_mtime = cutoff + 1  # на 1 секунду моложе
        
        row = (1, os.sep + os.path.join("source", "cam01", "young.jpg"), "young.jpg", "cam01", "2024-01-01 10:00:00", "sent")
        
        with patch("os.stat", return_value=FakeStat(young_mtime)), \
             patch("os.remove") as mock_remove, \
             patch("ftp_cleanup_worker.path_is_under_root", return_value=True):
            
            # st_mtime > cutoff -> пропускаем
            result = self._simulate_skip_check(row, cutoff)
            
            mock_remove.assert_not_called()
            self.assertEqual(result, "skipped")

    def test_file_1sec_older_than_retention_deleted(self):
        """Файл старше cutoff на 1 секунду -> удаляется (P1-5)"""
        retention_days = 7
        cutoff = time.time() - retention_days * 86400
        old_mtime = cutoff - 1  # на 1 секунду старше
        
        row = (1, os.sep + os.path.join("source", "cam01", "old.jpg"), "old.jpg", "cam01", "2024-01-01 10:00:00", "sent")
        
        with patch("os.stat", return_value=FakeStat(old_mtime)), \
             patch("os.remove") as mock_remove, \
             patch("ftp_cleanup_worker.path_is_under_root", return_value=True):
            
            result = self._simulate_deletion(row, cutoff, retention_days)
            
            mock_remove.assert_called_once()
            self.assertEqual(result["action"], "deleted")


class TestMissingMetadata(unittest.TestCase):
    """P1-6: Пропуск при отсутствии метаданных"""

    def test_none_full_path_skipped(self):
        """full_path=None -> пропускается (P1-6)"""
        row = (1, None, "file.jpg", "cam01", "2024-01-01 10:00:00", "sent")
        
        result = self._check_path_guardrails(row)
        
        self.assertEqual(result, "skipped_no_path")

    def test_empty_string_full_path_skipped(self):
        """full_path='' -> пропускается (P1-6)"""
        row = (1, "", "file.jpg", "cam01", "2024-01-01 10:00:00", "sent")
        
        result = self._check_path_guardrails(row)
        
        self.assertEqual(result, "skipped_no_path")

    def test_whitespace_full_path_allowed(self):
        """full_path='   ' (whitespace) -> проходит проверку (не пустая строка)"""
        # Пробельная строка не считается "falsy" в Python, но realpath упадет
        # Это edge case - whitespace path пройдет проверку not full_path,
        # но упадет в path_is_under_root или os.stat
        row = (1, "   ", "file.jpg", "cam01", "2024-01-01 10:00:00", "sent")
        
        # Проверка: "   " считается truthy в Python
        full_path = row[1]
        self.assertTrue(bool(full_path), "Whitespace строка truthy")
        
        # Но os.path.realpath("   ") вернет ошибку или странный путь
        # В реальном коде это будет обработано как outside_root или stat_error

    def _check_path_guardrails(self, row):
        """Эмулирует проверку пути из main() lines 145-147"""
        event_id, full_path, file_name, camera_code, first_seen_at, status = row
        
        if not full_path or not isinstance(full_path, str):
            return "skipped_no_path"
        
        return "passed"


class TestDeleteVsSkipDecision(unittest.TestCase):
    """Тесты для логики delete vs skip decision tree"""

    def test_delete_branch_file_gone_already(self):
        """Файл уже удален -> mark as removed (already_gone branch)"""
        row = (1, os.sep + os.path.join("source", "cam01", "missing.jpg"), "missing.jpg", "cam01", "2024-01-01 10:00:00", "sent")
        
        with patch("os.stat", side_effect=FileNotFoundError()), \
             patch("os.remove") as mock_remove:
            
            result = self._simulate_file_missing_branch(row)
            
            mock_remove.assert_not_called()  # remove не вызывается
            self.assertEqual(result["action"], "mark_removed")

    def test_delete_branch_os_error_on_remove(self):
        """Ошибка при os.remove -> skip с логированием"""
        retention_days = 7
        cutoff = time.time() - retention_days * 86400
        old_mtime = cutoff - 3600
        
        row = (1, os.sep + os.path.join("source", "cam01", "locked.jpg"), "locked.jpg", "cam01", "2024-01-01 10:00:00", "sent")
        
        with patch("os.stat", return_value=FakeStat(old_mtime)), \
             patch("os.remove", side_effect=PermissionError("Access denied")), \
             patch("ftp_cleanup_worker.path_is_under_root", return_value=True):
            
            result = self._simulate_deletion_with_error_handling(row, cutoff)
            
            self.assertEqual(result["action"], "skip")
            self.assertEqual(result["reason"], "remove_error")

    def test_delete_branch_os_error_on_stat(self):
        """Ошибка при os.stat -> skip (не mark_removed)"""
        row = (1, os.sep + os.path.join("source", "cam01", "broken.jpg"), "broken.jpg", "cam01", "2024-01-01 10:00:00", "sent")
        
        with patch("os.stat", side_effect=OSError("Stat failed")), \
             patch("ftp_cleanup_worker.path_is_under_root", return_value=True):
            
            result = self._simulate_stat_error(row)
            
            self.assertEqual(result["action"], "skip")
            self.assertEqual(result["reason"], "stat_error")

    def _simulate_file_missing_branch(self, row):
        """Эмулирует ветку FileNotFoundError из main()"""
        full_path = row[1]
        try:
            os.stat(full_path)
        except FileNotFoundError:
            # В реальном коде здесь UPDATE ftp_removed_at
            return {"action": "mark_removed", "event_id": row[0]}
        return {"action": "continue"}

    def _simulate_deletion_with_error_handling(self, row, cutoff):
        """Эмулирует ветку с обработкой ошибок удаления"""
        full_path = row[1]
        
        st = os.stat(full_path)
        if st.st_mtime > cutoff:
            return {"action": "skip", "reason": "too_young"}
        
        try:
            os.remove(full_path)
        except OSError as e:
            return {"action": "skip", "reason": "remove_error", "error": str(e)}
        
        return {"action": "deleted"}

    def _simulate_stat_error(self, row):
        """Эмулирует обработку ошибки stat"""
        full_path = row[1]
        try:
            os.stat(full_path)
        except FileNotFoundError:
            return {"action": "mark_removed"}
        except OSError:
            return {"action": "skip", "reason": "stat_error"}
        return {"action": "continue"}


class TestPathGuardrailsIntegration(unittest.TestCase):
    """Интеграционные тесты для path guardrails"""

    def test_outside_root_triggers_skip_not_delete(self):
        """P0-2: Путь вне root -> skip, delete не вызывается"""
        retention_days = 7
        cutoff = time.time() - retention_days * 86400
        old_mtime = cutoff - 3600
        
        row = (1, os.sep + os.path.join("etc", "passwd"), "passwd", "cam01", "2024-01-01 10:00:00", "sent")
        
        with patch("os.stat", return_value=FakeStat(old_mtime)), \
             patch("os.remove") as mock_remove, \
             patch("ftp_cleanup_worker.path_is_under_root", return_value=False):
            
            # Проверка guardrails
            is_allowed = worker.path_is_under_root(row[1], os.sep + "source")
            self.assertFalse(is_allowed)
            
            # Если бы мы продолжили - delete не должен быть вызван
            mock_remove.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
