import pandas as pd
from notificationtype import NotificationType


class ComparisonResult:
    
    """
    Holds result of comparing a single row in signplan with ratko-data.
    """
    def __init__(self, matches: list[str], row: pd.Series, row_idx: int, was_processed: bool = True):
        self.matches = matches
        self.notification_type = None
        self.note = None
        self.row = row
        self.row_idx = row_idx
        self.succesfully_processed = was_processed
        if self.succesfully_processed:
            self._construct(row)
        else:
            self.note = "Ei voitu prosessoida"

    def has_matches(self):
        return len(self.matches) > 0
    
    def has_unique_match(self):
        return len(self.matches) == 1
    
    def get_matches(self):
        return self.matches
    
    def get_notification(self):
        return self.notification_type
    
    def get_note(self):
        return self.note
    
    def set_note(self, note: str):
        self.note = note
    
    def get_row(self):
        return self.row
    
    def get_idx(self):
        return self.row_idx

    def _construct(self,row: pd.Series):
        """
        Forms note and based on the notification and matches.
        """

        from utils.file_utils import is_remove_operation

        should_be_removed = is_remove_operation(row)
        if self.matches:
            if len(self.matches) > 1:
                # Ambiguous. Should be handled manually.
                self.notification_type = NotificationType.NO_NOTIFICATION
                self.note = "Useita mätsejä"
            elif should_be_removed:
                # The sign is to be removed and OID was found. Create remove notification.
                # if matches and len(matches) > 1:
                self.notification_type = NotificationType.REMOVE_NOTIFICATION
                self.note = "Vastaavuus löytyi. Lisätty poistoilmoitusten listalle."
            else:
                    self.note = "Löytyy jo ratkosta. Ei tehdä mitään."
                    self.notification_type = NotificationType.NO_NOTIFICATION
        else:
            if not should_be_removed:
                # No matches and operation is something else than remove means that 
                # add notification should be created. No OID exists for the sign. 
                self.notification_type = NotificationType.ADD_NOTIFICATION
                self.note = "Ei löytynyt ratko-datasta. Lisäysilmoitus"
            else:
                self.note = "Ei löytynyt ratko-datasta ja kyseessä poistetaan toimenpide. Ei tehdä mitään"
                self.notification_type = NotificationType.NO_NOTIFICATION