from typing import Optional, Dict, Any

from pydantic import BaseModel


class U115Cookie(BaseModel):
    """
    115 Cookie 模型
    """
    UID: str
    CID: str
    SEID: str
    KID: Optional[str]


    @classmethod
    def from_string(cls, cookie_string: str) -> "U115Cookie":
        """
        解析 str Cookie
        """
        if not cookie_string or not isinstance(cookie_string, str):
            return cls()

        cookie_dict: Dict[str, Any] = {}
        parts = cookie_string.strip().rstrip(';').split(';')

        for part in parts:
            if '=' in part:
                key, value = part.split('=', 1)
                key = key.strip()
                value = value.strip()
                if key in cls.__fields__:
                    cookie_dict[key] = value

        return cls(**cookie_dict)

    def to_dict(self) -> Dict[str, Any]:
        """
        转换成 Dict 返回
        """
        cookie_dict: Dict[str, Any] = {
            "UID": self.UID,
            "CID": self.CID,
            "SEID": self.SEID,
        }
        if self.KID:
            cookie_dict["KID"] = self.KID
        return cookie_dict
