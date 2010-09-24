# -*- coding: utf-8 -*-


import sqlite3


class transformer:
    def __init__(self, sq_connection):
        if sq_connection.__class__ != sqlite3.Connection:
            raise Exception("sq_connection is not sqlite3.Connection instance")

    
