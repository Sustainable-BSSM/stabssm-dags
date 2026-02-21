from typing import Any, Dict, List


class DictUtils:

    @staticmethod
    def get_nested(d: Dict[str, Any], path: List[str], default=None):
        """중첩된 딕셔너리에서 경로를 따라 값을 가져오는 함수.
        Args:
            d: 대상 딕셔너리
            path: 키 경로 리스트 (예: ["data", "items", "name"])
            default: 경로가 없을 경우 반환할 기본값

        Returns:
            경로에 해당하는 값 또는 default
        """
        cur = d
        for p in path:
            if not isinstance(cur, dict) or p not in cur:
                return default
            cur = cur[p]
        return cur
