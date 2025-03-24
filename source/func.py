import pandas as pd
import random

def extract_category(name_product):
    name_lower = name_product.lower()
    if any (keyword in name_lower for keyword in ['laptop', 'sleeve']):
        return 'TAS LAPTOP'
    elif any (keyword in name_lower for keyword in ['sling bag', 'sling pouch']):
        return 'TAS SLING BAG'
    elif any (keyword in name_lower for keyword in ['backpack', 'rucksack']):
        return 'TAS BACKPACK'
    elif any (keyword in name_lower for keyword in ['pouch']):
        return 'TAS POUCH'
    elif any (keyword in name_lower for keyword in ['tote bag']):
        return 'TAS TOTE BAG'
    else:
        return 'SEMUA ETALASE'

def extract_color(name_product):
    color_list = [
        'DARK GREEN', 'SKY BLUE', 'BIRU MUDA', 'BLUE', 'ORANGE', 'LIGHT YELLOW', 'GREEN ARMY', 'FUCHSIA',
        'LIGHT BROWN', 'DARK BLUE', 'MINT GREEN', 'DARK GREY', 'UNGU MUDA', 'FUCSHIA', 'LIGHT PINK', 'CREAM',
        'GOLD', 'DARK OLIVE', 'RED', 'GREY', 'WHITE', 'DARK PURPLE', 'SALEM', 'FUSCHIA', 'HITAM', 'LIME',
        'BROWN', 'BLUEBERRY', 'SOFT BLUE', 'BLACK', 'DARK SALEM', 'YELLOW', 'MAROON', 'DARK BROWN', 'BEIGE',
        'LIGHT BLUE', 'GREEN', 'COKELAT MUDA', 'LIGHT GREEN', 'CURRY', 'PINK', 'KREM', 'BIRU TUA', 'KHAKI',
        'PURPLE', 'EMERALD GREEN', 'DARK ORANGE', 'FUCHIA', 'NAVY', 'LIGHT PURPLE', 'OLIVE', 'LIGHT GREY'
    ]

    color_translation = {
        'KREM': 'CREAM',
        'COKELAT MUDA': 'LIGHT BROWN',
        'BIRU TUA': 'DARK BLUE',
        'HITAM': 'BLACK',
        'BIRU MUDA': 'LIGHT BLUE',
        'SALEM': 'SALMON',
        'UNGU MUDA': 'LIGHT PURPLE',
        'FUCSHIA': 'FUCHSIA',
        'FUCHSIA': 'FUCHSIA',
        'FUCHIA': 'FUCHSIA',
    }

    name_product = str(name_product).strip()

    # Konversi ke huruf besar
    name_upper = name_product.upper()

    # Ambil warna setelah tanda "-"
    color_product = name_upper.split('-')[-1].strip()

    # Urutan pengecekan:
    if color_product in color_translation:  # 1. Cek di color_translation dulu
        return color_translation[color_product]
    elif color_product in color_list:  # 2. Cek di color_list
        return color_product
    else:  # 3. Jika tidak ada di keduanya
        return 'Tidak ada spesifikasi warna'

def generate_product_id(index):
    letters = "ETWS"  # Menggunakan ETWS secara berurutan
    numbers = ''.join(random.choices("0123456789", k=3))  # 3 angka acak
    
    return letters + numbers

def generate_color_id(index):
    letters = "ECLR"  # Menggunakan ECLR secara berurutan
    numbers = ''.join(random.choices("0123456789", k=3))  # 3 angka acak
    
    return letters + numbers

def generate_category_id(index):
    letters = "ECAT" # Menggunakan ECAT secara berurutan
    numbers = ''.join(random.choices("0123456789", k=3))  # 3 angka acak  