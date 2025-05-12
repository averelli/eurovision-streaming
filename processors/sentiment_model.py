import spacy
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import logging
from entity_patterns import patterns


class SentimentService:
    """
    Loads models once, keeps them alive, and provides methods
    to analyze single texts or batches quickly.
    """
    def __init__(self, logger:logging.Logger, model_name="cardiffnlp/twitter-roberta-base-sentiment"):
        self.logger = logger
        # load spacy once
        self.nlp = spacy.load("en_core_web_sm")
        # add custom patterns
        ruler = self.nlp.add_pipe("entity_ruler", before="ner", config={"overwrite_ents": True})  # note: string name, not object
        ruler.add_patterns(patterns)

        # load hf tokenizer + model + pipeline once
        tokenizer = AutoTokenizer.from_pretrained(model_name)
        model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.sentiment_pipeline = pipeline(
            "sentiment-analysis",
            model=model,
            tokenizer=tokenizer,
            device=0  
        )

    def extract_tags(self, doc):
        """
        Returns set of countries/participants/song titles mentioned.
        Entities come from your entity_ruler: labels = CONTESTANT, SONG, plus GPE for country.
        """
        # detect tags
        tags = set()
        for ent in doc.ents:
            if ent.label_ in {"CONTESTANT", "SONG", "GPE"}:
                tags.add((ent.text, ent.label_))

        # if tags are empty - assign general tag
        if not tags:
            tags.add(("general", "GEN"))
        return tags

    def split_clauses(self, sent):
        """
        Only split if more than one distinct country/participant tag in the sentence.
        Otherwise return [sent.text].
        """
        tags = self.extract_tags(sent)
        # collect just country/contestant texts
        tag_texts = {t for t, label in tags if label in {"CONTESTANT", "GPE", "SONG"}}

        # if 0 or 1 tag - no need to split
        if len(tag_texts) <= 1:
            return [sent.text]

        # if >1 tags → split on coordinating conj linking clauses
        clauses, current = [], []
        for token in sent:
            current.append(token.text)
            if token.dep_ == "cc" and token.lower_ in {"but","however","yet"}:
                clauses.append(" ".join(current[:-1]).strip())
                current = []
        if current:
            clauses.append(" ".join(current).strip())
        return clauses

    def analyze_sentiment(self, texts):
        """Batch or single sentiment call."""
        out = self.sentiment_pipeline(texts if isinstance(texts, list) else [texts])
        # ensure list & round
        return [{"label": r["label"], "score": round(r["score"], 3)} for r in out]

    def process_post(self, post: dict) -> dict:
        """
        post should contain at least:
          { "post_id", "timestamp", "post_author", "platform", "raw_html" }
        Returns enriched dict with:
          tags, clauses[ { text, sentiment, clause_tags } ], overall_vibe
        """
        
        doc = self.nlp(post["raw_html"])

        # post level tags
        post_tags = self.extract_tags(doc)

        # per sentence and clause splitting
        clauses_info = []
        overall_scores = []
        for sent in doc.sents:
            sub_clauses = self.split_clauses(sent)
            # sentiment over clauses
            scores = self.analyze_sentiment(sub_clauses)
            for clause_text, sent_res in zip(sub_clauses, scores):
                # extract tags in this clause
                clause_doc = self.nlp(clause_text)
                clause_tags = self.extract_tags(clause_doc)
                clauses_info.append({
                    "text": clause_text,
                    "sentiment": sent_res,
                    "clause_tags": list(clause_tags)
                })
                overall_scores.append(sent_res["score"] * (1 if sent_res["label"]=="LABEL_2" else -1))

        # overall score: average signed score mapped to int [-1,1]
        if overall_scores:
            avg = sum(overall_scores) / len(overall_scores)
            post_score = round(avg, 2)
        else:
            post_score = 0

        return {
            "post_id": post["post_id"],
            "timestamp": post["timestamp"],
            "post_author": post["post_author"],
            "platform": post["platform"],
            "raw_text": post["raw_html"],
            "tags": [ {"text": t, "label": l} for t,l in post_tags ],
            "clauses": clauses_info,
            "overall_vibe": post_score
        }
