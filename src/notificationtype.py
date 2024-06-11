from enum import Enum

class NotificationType(Enum):

    ADD_NOTIFICATION = 0,
    CHANGE_NOTIFICATION = 1,
    REMOVE_NOTIFICATION = 2,
    NO_NOTIFICATION = 3,

    @staticmethod
    def to_filename(type):
        type_to_filename =  { 
            NotificationType.ADD_NOTIFICATION: "lisaysilmoitukset.csv",
            NotificationType.CHANGE_NOTIFICATION: "muutosilmoitukset.csv",
            NotificationType.REMOVE_NOTIFICATION: "poistoilmoitukset.csv"
            }
        
        return type_to_filename[type]