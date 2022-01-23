from enum import Enum
import numbers

class EnumString(Enum):
    @classmethod
    def from_str(cls, value):
        for k, v in cls.__members__.items():
            if v.value == value:
                return v
        else:
            raise ValueError(f"'{cls.__name__}' enum not found for '{value}'")

    def __eq__(self, other):
        return self.value == other.value

    def __hash__(self):
        return hash(self.value)

class EnumComparable(Enum):
    def __hash__(self):
        return hash(self.value)

    def __gt__(self, other):
        try:
            return self.value > other.value
        except:
            pass
        try:
            if isinstance(other, numbers.Real):
                return self.value > other
        except:
            pass
        return NotImplemented

    def __lt__(self, other):
        try:
            return self.value < other.value
        except:
            pass
        try:
            if isinstance(other, numbers.Real):
                return self.value < other
        except:
            pass
        return NotImplemented

    def __ge__(self, other):
        try:
            return self.value >= other.value
        except:
            pass
        try:
            if isinstance(other, numbers.Real):
                return self.value >= other
            if isinstance(other, str):
                return self.name == other
        except:
            pass
        return NotImplemented

    def __le__(self, other):
        try:
            return self.value <= other.value
        except:
            pass
        try:
            if isinstance(other, numbers.Real):
                return self.value <= other
            if isinstance(other, str):
                return self.name == other
        except:
            pass
        return NotImplemented

    def __eq__(self, other):
        try:
            return self.value == other.value
        except:
            pass
        try:
            if isinstance(other, numbers.Real):
                return self.value == other
            if isinstance(other, str):
                return self.name == other
        except:
            pass
        return NotImplemented


