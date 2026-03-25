#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Needs freetype-py>=1.0

import freetype


class Bitmap(object):
    def __init__(self, width, height, pixels=None):
        self.width = width
        self.height = height
        self.pixels = pixels or bytearray(width * height)


class Font(object):
    def __init__(self, filename, size):
        self.face = freetype.Face(filename)
        self.face.set_pixel_sizes(0, size)

    def glyph_for_character(self, char):
        self.face.load_char(char, freetype.FT_LOAD_RENDER | freetype.FT_LOAD_TARGET_MONO)
        return self.face.glyph


if __name__ == '__main__':
    fnt = Font('helvetica.ttf', 24)
