# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pandas as pd
from JDSpider.items import *

class JdspiderPipeline(object):
    def __init__(self):
        self.Categories = pd.DataFrame()
        self.Products = pd.DataFrame()
        self.Shop = pd.DataFrame()
        self.Comment = pd.DataFrame()
        self.CommentImage = pd.DataFrame()
        self.CommentSummary = pd.DataFrame()
        self.HotCommentTag = pd.DataFrame()

    def close_spider(self, spider):
        self.Categories.to_csv("categories.csv", sep='\t')
        self.Products.to_csv("products.csv", sep='\t')
        self.Shop.to_csv("shop.csv", sep='\t')
        self.Comment.to_csv("comment.csv", sep='\t')
        self.CommentImage.to_csv("commentImage.csv", sep='\t')
        self.CommentSummary.to_csv("commentSummary.csv", sep='\t')
        self.HotCommentTag.to_csv("hotCommentTag.csv", sep='\t')

    def process_item(self, item, spider):
        """ 判断item的类型，并作相应的处理，再入数据库 """
        if isinstance(item, CategoriesItem):
            try:
                self.Categories.append(pd.DataFrame(dict(item).items(), columns=dict(item).keys()))
            except Exception:
                pass
        elif isinstance(item, ProductsItem):
            try:
                self.Products.append(pd.DataFrame(dict(item).items(), columns=dict(item).keys()))
            except Exception:
                pass
        elif isinstance(item, ShopItem):
            try:
                self.Shop.append(pd.DataFrame(dict(item).items(), columns=dict(item).keys()))
            except Exception:
                pass
        elif isinstance(item, CommentItem):
            try:
                self.Comment.append(pd.DataFrame(dict(item).items(), columns=dict(item).keys()))
            except Exception:
                pass
        elif isinstance(item, CommentImageItem):
            try:
                self.CommentImage.append(pd.DataFrame(dict(item).items(), columns=dict(item).keys()))
            except Exception:
                pass
        elif isinstance(item, CommentSummaryItem):
            try:
                self.CommentSummary.append(pd.DataFrame(dict(item).items(), columns=dict(item).keys()))
            except Exception:
                pass
        elif isinstance(item, HotCommentTagItem):
            try:
                self.HotCommentTag.append(pd.DataFrame(dict(item).items(), columns=dict(item).keys()))
            except Exception:
                pass
        return item