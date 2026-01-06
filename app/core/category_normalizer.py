"""Category normalization utility for news sources."""

from typing import Optional
from app.core.logging import setup_logging

logger = setup_logging()

# Normalized category set (enum-like)
NORMALIZED_CATEGORIES = {
    "politics",
    "economy",
    "society",
    "international",
    "culture",
    "sports",
    "science",
    "technology",
    "health",
    "provinces",
    "other",
}

# Category mappings per source
# Format: {source_name: {raw_category: normalized_category}}
CATEGORY_MAPPINGS = {
    "mehrnews": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "سیاست‌های داخلی": "politics",
        "سیاست‌های خارجی": "politics",
        "داخلی": "politics",
        "خارجی": "international",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "اقتصاد و تجارت": "economy",
        "بازار": "economy",
        "بورس": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        "حقوقی": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        "خارجی": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "دين، حوزه، انديشه": "culture",
        "هنر": "culture",
        "سینما": "culture",
        "موسیقی": "culture",
        "ادبیات": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        "والیبال": "sports",
        "بسکتبال": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "پزشکی": "health",
        "سلامت": "health",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "فناوری اطلاعات": "technology",
        "IT": "technology",
        "رایانه": "technology",
        # Provinces
        "استانها": "provinces",
    },
    "isna": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "داخلی": "politics",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "بازار": "economy",
        "بورس": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        "خارجی": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "هنر": "culture",
        "سینما": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "پزشکی": "health",
        "سلامت": "health",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "فناوری اطلاعات": "technology",
        # Provinces
        "استان ها": "provinces",
        "استانها": "provinces",
    },
    "irna": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "داخلی": "politics",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "بازار": "economy",
        "بورس": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        "خارجی": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "دين، حوزه، انديشه": "culture",
        "هنر": "culture",
        "سینما": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "پزشکی": "health",
        "سلامت": "health",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "فناوری اطلاعات": "technology",
        # Provinces
        "استان ها": "provinces",
        "استانها": "provinces",
        "استان‌ها": "provinces",  # With zero-width non-joiner
    },
    "fars": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "داخلی": "politics",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "بازار": "economy",
        "بورس": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        "خارجی": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "هنر": "culture",
        "سینما": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "پزشکی": "health",
        "سلامت": "health",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "فناوری اطلاعات": "technology",
        # Provinces
        "استان ها": "provinces",
        "استانها": "provinces",
        "استان‌ها": "provinces",  # With zero-width non-joiner
    },
    "tasnim": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست ایران": "politics",
        "نظامی | دفاعی | امنیتی": "politics",
        "گزارش و تحلیل سیاسی": "politics",
        "مجلس و دولت": "politics",
        "امام و رهبری": "politics",
        "دفاعی و امنیتی": "politics",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "اقتصاد ایران": "economy",
        "پول | ارز | بانک": "economy",
        "خودرو": "economy",
        "صنعت و تجارت": "economy",
        "نفت و انرژی": "economy",
        "بازار سهام | بورس": "economy",
        "کشاورزی": "economy",
        "اقتصاد جهان": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "پزشکی": "health",
        "طب سنتی": "health",
        "خانواده و جوانان": "society",
        "تهران": "society",
        "فرهنگیان و مدارس": "society",
        "پلیس": "society",
        "حقوقی و قضایی": "society",
        "علم و تکنولوژی": "science",
        "محیط زیست": "society",
        "سفر": "society",
        "حوادث": "society",
        "آسیب های اجتماعی": "society",
        "بازنشستگان": "society",
        # International
        "بین الملل": "international",
        "بین‌الملل": "international",
        "دیپلماسی ایران": "international",
        "تولیدات دفاتر خارجی": "international",
        "آسیای غربی": "international",
        "افغانستان": "international",
        "آمریکا": "international",
        "اروپا": "international",
        "آسیا-اقیانوسیه": "international",
        "پاکستان و هند": "international",
        "ترکیه و اوراسیا": "international",
        "آفریقا": "international",
        "بیداری اسلامی": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "ادبیات و نشر": "culture",
        "رادیو و تلویزیون": "culture",
        "دین ، قرآن و اندیشه": "culture",
        "سینما و تئاتر": "culture",
        "فرهنگ حماسه و مقاومت": "culture",
        "موسیقی و تجسمی": "culture",
        "حوزه و روحانیت": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال ایران": "sports",
        "فوتبال جهان": "sports",
        "والیبال | بسکتبال | هندبال": "sports",
        "کشتی و وزنه‌برداری": "sports",
        "ورزش های رزمی": "sports",
        "ورزش زنان": "sports",
        "ورزش جهان": "sports",
        "رشته های ورزشی": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "فضا و نجوم": "science",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "فناوری اطلاعات | اینترنت | موبایل": "technology",
        # Health
        "سلامت": "health",
        "پزشکی": "health",
        # Provinces
        "استانها": "provinces",
        "استان‌ها": "provinces",
        "استان ها": "provinces",
    },
    "iribnews": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "داخلی": "politics",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "بازار": "economy",
        "بورس": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        "خارجی": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "هنر": "culture",
        "سینما": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "پزشکی": "health",
        "سلامت": "health",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "فناوری اطلاعات": "technology",
        # Provinces
        "استانها": "provinces",
        "استان‌ها": "provinces",
        "استان ها": "provinces",
        "مازندران": "provinces",
        "تهران": "provinces",
        "اصفهان": "provinces",
        "فارس": "provinces",
        "خراسان": "provinces",
        "آذربایجان": "provinces",
        "گیلان": "provinces",
        "خوزستان": "provinces",
        "کرمان": "provinces",
        "آذربایجان شرقی": "provinces",
        "آذربایجان غربی": "provinces",
        "خراسان رضوی": "provinces",
        "خراسان شمالی": "provinces",
        "خراسان جنوبی": "provinces",
    },
    "ilna": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "داخلی": "politics",
        "خارجی": "international",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "اقتصاد و تجارت": "economy",
        "بازار": "economy",
        "بورس": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        "حقوقی": "society",
        "کارگری": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "هنر": "culture",
        "سینما": "culture",
        "موسیقی": "culture",
        "ادبیات": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "پزشکی": "health",
        "سلامت": "health",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "IT": "technology",
        "رایانه": "technology",
        # Provinces
        "استانها": "provinces",
        "استان‌ها": "provinces",
        "استان ها": "provinces",
    },
    "mizan": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "داخلی": "politics",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "قضایی": "society",
        "حقوق بشر": "society",
        "محاکمه منافقین": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        # Multimedia
        "چندرسانه": "culture",
        "چندرسانه‌ای": "culture",
        "عمومی": "other",
        "عکس": "culture",
    },
    "varzesh3": {
        # Sports (all Varzesh3 news is sports-related)
        "sports": "sports",  # Default category for all Varzesh3 news
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        "فوتبال ایران": "sports",
        "فوتبال جهان": "sports",
        "لیگ برتر": "sports",
        "لیگ برتر ایران": "sports",
        "لیگ برتر انگلیس": "sports",
        "لالیگا": "sports",
        "سری آ": "sports",
        "نقل و انتقالات": "sports",
        "جام جهانی": "sports",
        "جام ملت‌های آسیا": "sports",
        "لیگ قهرمانان": "sports",
        "لیگ قهرمانان اروپا": "sports",
        "لیگ قهرمانان آسیا": "sports",
        "والیبال": "sports",
        "بسکتبال": "sports",
        "هندبال": "sports",
        "کشتی": "sports",
        "وزنه‌برداری": "sports",
        "ورزش‌های رزمی": "sports",
        "فوتسال": "sports",
        "تنیس": "sports",
        "بازی‌های آسیایی": "sports",
        "المپیک": "sports",
    },
    "mashreghnews": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "داخلی": "politics",
        "خارجی": "international",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "اقتصاد و تجارت": "economy",
        "بازار": "economy",
        "بورس": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        "حقوقی": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "هنر": "culture",
        "سینما": "culture",
        "موسیقی": "culture",
        "ادبیات": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "پزشکی": "health",
        "سلامت": "health",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "فناوری اطلاعات": "technology",
        "IT": "technology",
        "رایانه": "technology",
        # Defense/Military
        "دفاعی": "politics",
        "نظامی": "politics",
        "جهاد و مقاومت": "politics",
        # Provinces
        "استانها": "provinces",
        "استان‌ها": "provinces",
        "استان ها": "provinces",
    },
    "yjc": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "داخلی": "politics",
        "خارجی": "international",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "اقتصاد و تجارت": "economy",
        "بازار": "economy",
        "بورس": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        "حقوقی": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "هنر": "culture",
        "سینما": "culture",
        "موسیقی": "culture",
        "ادبیات": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "پزشکی": "health",
        "سلامت": "health",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "فناوری اطلاعات": "technology",
        "IT": "technology",
        "رایانه": "technology",
        # Defense/Military
        "دفاعی": "politics",
        "نظامی": "politics",
        # Provinces
        "استانها": "provinces",
        "استان‌ها": "provinces",
        "استان ها": "provinces",
    },
    "iqna": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "داخلی": "politics",
        "خارجی": "international",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "اقتصاد و تجارت": "economy",
        "بازار": "economy",
        "بورس": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        "حقوقی": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "هنر": "culture",
        "سینما": "culture",
        "موسیقی": "culture",
        "ادبیات": "culture",
    },
    "hamshahri": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "اخبار سیاسی ایران": "politics",
        "دفاع-امنیت": "politics",
        "دفاع امنیت": "politics",
        "امنیت": "politics",
        "دفاع": "politics",
        "داخلی": "politics",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "اقتصاد و تجارت": "economy",
        "بازار": "economy",
        "بورس": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        "حقوقی": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        "اخبار جهان": "international",
        "آمریکا": "international",
        "روسیه‌ و قفقاز": "international",
        "روسیه و قفقاز": "international",
        "اروپا": "international",
        "خاورمیانه": "international",
        "آسیا": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "هنر": "culture",
        "سینما": "culture",
        "موسیقی": "culture",
        "ادبیات": "culture",
        "چندرسانه‌ای": "culture",
        "ویدئو": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "فناوری": "technology",
        "تکنولوژی": "technology",
        # Health
        "سلامت": "health",
        "بهداشت": "health",
        # Provinces
        "استان‌ها": "provinces",
        "استانها": "provinces",
        "استان ها": "provinces",
    },
    "donyaeqtesad": {
        # Economy (main focus)
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "اقتصاد و تجارت": "economy",
        "بازار": "economy",
        "بورس": "economy",
        "بورس و سرمایه": "economy",
        "بازار سرمایه": "economy",
        "بازار پول": "economy",
        "بازار ارز": "economy",
        "بازار طلا": "economy",
        "بازار مسکن": "economy",
        "بازار خودرو": "economy",
        "انرژی": "economy",
        "نفت و گاز": "economy",
        "صنعت و تجارت": "economy",
        "صنعت": "economy",
        "تجارت": "economy",
        "بانک و بیمه": "economy",
        "بانک": "economy",
        "بیمه": "economy",
        "اقتصاد کلان": "economy",
        "اقتصاد خرد": "economy",
        "مالی": "economy",
        "پولی": "economy",
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "داخلی": "politics",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        "خارجی": "international",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        "حقوقی": "society",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "فناوری اطلاعات": "technology",
        "IT": "technology",
        "رایانه": "technology",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "هنر": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        # Health
        "سلامت": "health",
        "بهداشت": "health",
        "پزشکی": "health",
    },
    "snn": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "داخلی": "politics",
        "خارجی": "international",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "اقتصاد و تجارت": "economy",
        "بازار": "economy",
        "بورس": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        "حقوقی": "society",
        "دانشجویی": "society",
        "دانشگاه": "society",
        "آموزش": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "هنر": "culture",
        "سینما": "culture",
        "موسیقی": "culture",
        "ادبیات": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "دانش": "science",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "فناوری اطلاعات": "technology",
        "IT": "technology",
        # Health
        "سلامت": "health",
        "بهداشت": "health",
        "پزشکی": "health",
        # Provinces
        "استان‌ها": "provinces",
        "استانها": "provinces",
        "استان ها": "provinces",
        # Religion/Spiritual (specific to IQNA)
        "دینی": "culture",
        "مذهبی": "culture",
        "قرآن": "culture",
        "حدیث": "culture",
        "فقه": "culture",
        "اخلاق": "culture",
        "معارف": "culture",
        "معارف اسلامی": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "پزشکی": "health",
        "سلامت": "health",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "فناوری اطلاعات": "technology",
        "IT": "technology",
        "رایانه": "technology",
        # Defense/Military
        "دفاعی": "politics",
        "نظامی": "politics",
        # Provinces
        "استانها": "provinces",
        "استان‌ها": "provinces",
        "استان ها": "provinces",
    },
    "kayhan": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "politics",
        "داخلی": "politics",
        "خارجی": "international",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "اقتصاد و تجارت": "economy",
        "بازار": "economy",
        "بورس": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        "حقوقی": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "هنر": "culture",
        "سینما": "culture",
        "موسیقی": "culture",
        "ادبیات": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        # Science
        "علمی": "science",
        "علم": "science",
        "پزشکی": "health",
        "سلامت": "health",
        # Technology
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "IT": "technology",
        "رایانه": "technology",
        # Provinces
        "استانها": "provinces",
        "استان‌ها": "provinces",
        "استان ها": "provinces",
    },
    "ipna": {
        # IPNA is a sports news agency - all categories map to sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        "فوتبال ایران": "sports",
        "فوتبال جهان": "sports",
        "فوتبال ساحلی": "sports",
        "فوتسال": "sports",
        "فوتبال پایه": "sports",
        "والیبال": "sports",
        "بسکتبال": "sports",
        "کشتی": "sports",
        "وزنه‌برداری": "sports",
        "تکواندو": "sports",
        "کاراته": "sports",
        "کاراته وان": "sports",
        "بوکس": "sports",
        "جودو": "sports",
        "شنا": "sports",
        "دوومیدانی": "sports",
        "المپیک": "sports",
        "پارالمپیک": "sports",
        "بازی‌های آسیایی": "sports",
        "لیگ برتر": "sports",
        "جام جهانی": "sports",
        "لیگ قهرمانان": "sports",
        # English sections
        "football": "sports",
        "volleyball": "sports",
        "basketball": "sports",
        "wrestling": "sports",
        "weightlifting": "sports",
        "futsal": "sports",
        "taekwondo": "sports",
        "karate": "sports",
        "other": "sports",
        "sports": "sports",
    },
    "tabnak": {
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        "سیاست داخلی": "politics",
        "سیاست خارجی": "international",
        "انتخابات": "politics",
        # Economy
        "اقتصادی": "economy",
        "اقتصاد": "economy",
        "بازار": "economy",
        "بورس": "economy",
        "صنعت و تجارت": "economy",
        "انرژی": "economy",
        "نفت و گاز": "economy",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        "حوادث": "society",
        "قضایی": "society",
        "حقوقی": "society",
        "آموزش": "society",
        # International
        "بین‌الملل": "international",
        "بین الملل": "international",
        "جهان": "international",
        "خارجی": "international",
        "آمریکا": "international",
        "اروپا": "international",
        "آسیا": "international",
        "خاورمیانه": "international",
        "عراق": "international",
        "سوریه": "international",
        "فلسطین": "international",
        # Culture
        "فرهنگی": "culture",
        "فرهنگ": "culture",
        "هنر": "culture",
        "سینما": "culture",
        "موسیقی": "culture",
        "کتاب": "culture",
        "ادبیات": "culture",
        "معماری": "culture",
        # Sports
        "ورزشی": "sports",
        "ورزش": "sports",
        "فوتبال": "sports",
        "لیگ برتر": "sports",
        "والیبال": "sports",
        "بسکتبال": "sports",
        "کشتی": "sports",
        "المپیک": "sports",
        # Science & Technology
        "علمی": "science",
        "علم": "science",
        "تحقیقات": "science",
        "فناوری": "technology",
        "تکنولوژی": "technology",
        "IT": "technology",
        "فضا": "science",
        "نجوم": "science",
        # Health
        "پزشکی": "health",
        "سلامت": "health",
        "بهداشت": "health",
        "درمان": "health",
        # Provinces
        "استان‌ها": "provinces",
        "استانها": "provinces",
        "استان ها": "provinces",
        "محلی": "provinces",
    },
    "eghtesadonline": {
        # Economy
        "اقتصاد": "economy",
        "اقتصادی": "economy",
        "بورس": "economy",
        "بازار": "economy",
        "صنعت": "economy",
        "کسب‌وکار": "economy",
        "مسکن": "economy",
        "انرژی": "economy",
        "نفت": "economy",
        "گاز": "economy",
        "بانک": "economy",
        "پول و بانک": "economy",
        "ارز": "economy",
        "طلا": "economy",
        # Technology
        "فناوری": "technology",
        "فن‌آوری": "technology",
        "دیجیتال": "technology",
        "اینترنت": "technology",
        "موبایل": "technology",
        # Sports
        "ورزش": "sports",
        "ورزشی": "sports",
        "فوتبال": "sports",
        # Culture
        "فرهنگ": "culture",
        "هنر": "culture",
        # Health
        "سلامت": "health",
        "بهداشت": "health",
        "پزشکی": "health",
        # Politics
        "سیاسی": "politics",
        "سیاست": "politics",
        # International
        "بین‌الملل": "international",
        "جهان": "international",
        "خارجی": "international",
        # Society
        "اجتماعی": "society",
        "جامعه": "society",
        # Provinces
        "استان‌ها": "provinces",
        "استانها": "provinces",
        "استان ها": "provinces",
        "محلی": "provinces",
    },
    "reuters_photos": {
        # All categories default to photos
        "عکس": "photos",
        "Photo": "photos",
        "Photos": "photos",
        "Picture": "photos",
        "Pictures": "pictures",
        "Image": "photos",
        "Images": "photos",
        # But also support other categories if provided
        "Politics": "politics",
        "Economy": "economy",
        "Sports": "sports",
        "Technology": "technology",
        "Health": "health",
        "Culture": "culture",
        "International": "international",
        "Society": "society",
    },
    "reuters_text": {
        # English categories from Reuters
        "Politics": "politics",
        "Political": "politics",
        "Government": "politics",
        "Economy": "economy",
        "Economic": "economy",
        "Business": "economy",
        "Finance": "economy",
        "Markets": "economy",
        "Sports": "sports",
        "Sport": "sports",
        "Technology": "technology",
        "Tech": "technology",
        "Science": "science",
        "Health": "health",
        "Healthcare": "health",
        "Culture": "culture",
        "Arts": "culture",
        "Entertainment": "culture",
        "International": "international",
        "World": "international",
        "Asia": "international",
        "Europe": "international",
        "Americas": "international",
        "Middle East": "international",
        "Africa": "international",
        "Society": "society",
        "Social": "society",
    },
    "reuters_video": {
        # English categories from Reuters (same as text)
        "Politics": "politics",
        "Political": "politics",
        "Government": "politics",
        "Economy": "economy",
        "Economic": "economy",
        "Business": "economy",
        "Finance": "economy",
        "Markets": "economy",
        "Sports": "sports",
        "Sport": "sports",
        "Technology": "technology",
        "Tech": "technology",
        "Science": "science",
        "Health": "health",
        "Healthcare": "health",
        "Culture": "culture",
        "Arts": "culture",
        "Entertainment": "culture",
        "International": "international",
        "World": "international",
        "Asia": "international",
        "Europe": "international",
        "Americas": "international",
        "Middle East": "international",
        "Africa": "international",
        "Society": "society",
        "Social": "society",
        "Domestic": "politics",
        "National": "politics",
        # Video-specific
        "ویدئو": "videos",
        "Video": "videos",
        "Videos": "videos",
        # Default to international for Reuters video
        "News": "international",
        "Top News": "international",
        "Breaking News": "international",
        "Domestic": "politics",
        "National": "politics",
        # Default to international for Reuters text
        "News": "international",
        "Top News": "international",
        "Breaking News": "international",
    },
}


def normalize_category(source: str, raw_category: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Normalize category from raw source category.
    
    Args:
        source: News source name (e.g., "mehrnews", "isna")
        raw_category: Original category string from source
        
    Returns:
        Tuple of (normalized_category, raw_category)
        - normalized_category: Normalized category or "other" if unknown
        - raw_category: Original category (preserved)
    """
    if not raw_category or not raw_category.strip():
        return None, None
    
    raw_category = raw_category.strip()
    
    # Get mappings for this source
    source_mappings = CATEGORY_MAPPINGS.get(source, {})
    
    # Try exact match first
    normalized = source_mappings.get(raw_category)
    
    # If not found, try case-insensitive match
    if not normalized:
        for raw_key, norm_value in source_mappings.items():
            if raw_key.lower() == raw_category.lower():
                normalized = norm_value
                break
    
    # If still not found, try partial match (contains)
    if not normalized:
        raw_lower = raw_category.lower()
        for raw_key, norm_value in source_mappings.items():
            if raw_key.lower() in raw_lower or raw_lower in raw_key.lower():
                normalized = norm_value
                break
    
    # If still not found, check if it contains subcategory separator
    if not normalized and '>' in raw_category:
        # Extract main category (before '>')
        main_cat = raw_category.split('>')[0].strip()
        normalized = source_mappings.get(main_cat)
        if not normalized:
            # Try case-insensitive match for main category
            main_cat_lower = main_cat.lower()
            for raw_key, norm_value in source_mappings.items():
                if raw_key.lower() == main_cat_lower:
                    normalized = norm_value
                    break
    
    # Special handling for MehrNews: categories starting with "استانها"
    if not normalized and source == "mehrnews" and raw_category.startswith("استانها"):
        normalized = "provinces"
    
    # Special handling for IRNA: categories starting with "استان‌ها" or "استانها" or "استان ها"
    if not normalized and source == "irna":
        if raw_category.startswith("استان‌ها") or raw_category.startswith("استانها") or raw_category.startswith("استان ها"):
            normalized = "provinces"
    
    # Special handling for Varzesh3: all categories default to "sports"
    if not normalized and source == "varzesh3":
        normalized = "sports"
        logger.debug(
            f"Varzesh3 category '{raw_category}' mapped to 'sports'",
            extra={
                "source": source,
                "raw_category": raw_category,
                "normalized_category": normalized,
            }
        )
    
    # Default to "other" if no mapping found (for other sources)
    if not normalized:
        normalized = "other"
        logger.warning(
            f"Unknown category '{raw_category}' for source '{source}', mapped to 'other'",
            extra={
                "source": source,
                "raw_category": raw_category,
                "normalized_category": normalized,
            }
        )
    else:
        logger.debug(
            f"Category normalized: '{raw_category}' -> '{normalized}'",
            extra={
                "source": source,
                "raw_category": raw_category,
                "normalized_category": normalized,
            }
        )
    
    return normalized, raw_category

