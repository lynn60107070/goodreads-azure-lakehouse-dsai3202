# This is the most README README of all READMEs

Welcome to **Lab 4: Text Feature Engineering on Azure**, aka that one lab that aged us all 7 years in 3 days. If you’ve ever waited 20 hours for a Spark job and questioned all your life choices… same.

![valid crashout](images/me_rn.gif)


## I. The Premise (we did things, it hurt)
We took messy Goodreads reviews and turned them into **machine-readable pain**:
- `re` for regex exorcism
- `emoji` to neutralize 🥺✨
- `nltk` (VADER) to quantify 
- `scikit-learn` (TF-IDF) to mathematize rants
- Databricks to keep us humble (OOM: our recurring villain)

**By the end**:
- Cleaned + vectorized text feature matrix
- Proof that preprocessing choices change downstream performance
- Fresh **Gold/features_v2** datasets ready for modeling (aka the sequel)

## II. Split the Dataset (leakage who?)
**70 / 15 / 15** because data leakage is a bigger sin than lying on LinkedIn.

- **Train**: fit TF-IDF, any thresholds/scalers
- **Val**: tune features/models
- **Test**: do not touch. Ever.

Saved to:
/gold/features_v2/train
/gold/features_v2/val
/gold/features_v2/test

## III. Text Feature Extraction (my villain origin story)

### 1) Cleaning & Normalization
We detoxed `review_text` like it’s Twitter:
- lowercase
- URLs → `<URL>`
- numbers → `<NUM>`
- emojis → `<EMOJI>`
- punctuation out (kept `< >` for placeholders)
- trimmed; dropped reviews < 10 chars (goodbye ok)

We survived (barely).

### 2) Basic Text Features
- `review_length_words` how long the rant
- `review_length_chars` how dramatic the rant

### 3) Sentiment (VADER: the therapist we didn’t ask for)
- `sentiment_pos`, `sentiment_neu`, `sentiment_neg`, `sentiment_compound`
- Sometimes cleaning yeeted the emotional words, so VADER said "meh" (0.0). It be like that.

### 4) TF-IDF (math but make it sad)
- `ngram_range=(1,2)` to catch "not good" and "very mid"
- `max_features=1000` because Spark said *no more pls*
- `stop_words='english'`

Each review → a 1000-dimensional anxiety array. We saved:
/gold/features_v2/train_text_tfidf
(/val_text_tfidf, /test_text_tfidf when finalized)


### 5) Extra Features (side quests we chose anyway)
- `ttr` (type–token ratio), `avg_word_len`
- `emoji_count`, `url_count`, `negation_count`
- `elongated_count` (soooooo), `exclaim_count`, `question_count`
- `punct_ratio` (!!! per character)

cuz quantifying vibes is a valid research methodology now

## IV. The Big Merge (feature soup)
We assembled **metadata + basic + sentiment + extra + TF-IDF (+ embeddings if available)** into a single cursed but beautiful table per split


**Front columns**: `review_id, book_id, rating`  
**Then**: `review_length_*`, `sentiment_*`, extras  
**Finally**: `tfidf_features` (+ `bert_embedding` if computed)

Schema is consistent across splits. Rows align by `review_id`. We checked. Twice. (Trauma.)

## V. What Actually Worked (lessons learned)
- Cleaning must be **identical** across splits; only *fit* on train
- Deterministic sampling beats chaos (hash by `review_id`)
- Keep negations if you care about not good
- Python UDFs are slow; Pandas UDFs = fewer tears
- Save artifacts (vocab + params) or future-you will fight future-you

## VI. Repo & Branch (teacher pls look)
- Branch: `text_feature_extraction`
- Includes notebooks, feature outputs, and enough comments to pass a Turing test
- All Delta tables under `/gold/features_v2/...`

## VII. Credits (cast & crew)
- Me vs. Databricks clusters (final score: 2–10000)
- VADER for the free therapy
- TF-IDF for the 1000-D existential crisis
- Regex for being both hero and villain

## VIII. Epilogue
This lab felt like a three-hour art film where the protagonist fights Spark OOM errors and wins by sheer stubbornness. 

![absolute cinema](images/GIVEMEABREAK.jpeg)



