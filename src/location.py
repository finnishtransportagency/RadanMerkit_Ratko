from functools import total_ordering

@total_ordering
class LocationPoint:

    """
    Represents a location on a track.
    """

    def __init__(self,km, m):
        self.km = km
        self.m = m

    def __eq__(self,other):
        return (self.km, self.m) == (other.km, other.m)

    def __lt__(self,other):
        return self.km < other.km or  (self.km == other.km and self.m < other.m)

    def __str__(self):
        km_str = str(self.km)
        m_str = str(self.m)
        
        return (4- len(km_str)) * "0" + km_str + "+" + (4-len(m_str)) *"0" + m_str

    def meters(self):
        return self.m

    def kilometers(self):
        return self.km
    
    def create_from_this(self, add_meters):

        """
        Creates a new location point with added_meters (can be negative).
        Does not change kilometers
        """

        return LocationPoint(self.km,self.m+add_meters)
        
    @staticmethod
    def from_str(point: str):

        parts = point.split("+") # point is in format 0xxx+0xxx
        km = int(parts[0])
        m = int(parts[1])

        return LocationPoint(km,m)