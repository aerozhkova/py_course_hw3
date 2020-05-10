from PIL import Image


def transpose(img, loc_url):
    image = Image.open(loc_url + img)
    out = image.transpose(Image.FLIP_LEFT_RIGHT)
    out.filename = "___" + img
    return out
