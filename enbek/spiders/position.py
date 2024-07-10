import scrapy


class PositionSpider(scrapy.Spider):
    name = "position"
    allowed_domains = ["enbek.kz"]
    start_urls = ["https://enbek.kz/ru/search/vacancy?except[subsidized]=subsidized&region_id=75",]

    def parse(self, response):

        position_page_links = response.css(".item-list  a::attr(href)")
        yield from response.follow_all(position_page_links, self.parse_position)

        next_page_number = response.css("li.next button::attr(data-page)").get()
        if next_page_number!=0:
            next_page = self.start_urls[0]+"&page="+next_page_number
            print(next_page)
            yield response.follow(next_page, callback=self.parse)

    def parse_position(self, response):
        def extract_with_css(query):
            return response.css(query).get(default="").strip()


        item={}
        item['url'] = response.request.url
        item['publish_date'] = extract_with_css("ul.info.small.mb-2 li::text")
        item["title"]= extract_with_css("h4.title strong::text"),
        item["salary"]= extract_with_css(".price::text"),
        item["field"]= extract_with_css(".category.mb-2::text"),
        item["employer_description"]= extract_with_css("div.head div div.category.mb-2::text"),
        item['employer']=extract_with_css("div.head div div.title a::text"),
        item['headcount'] = extract_with_css("div.item-list.pea p::text"),
        raw=response.css("ul.info.d-flex.flex-column li span *::text").extract()
        info_blocks = response.css("div.single-line")
        info = {}
        for block in info_blocks:
            labels = block.css("div.label ::text").extract()
            values = block.css("div.value ::text").extract()
            values2 = [str(x).strip() for x in values]
            values2 = [x for x in values2 if len(x) > 0]
            values2 = "".join(values2)
            info[labels[0]]=values2
        info["Знание языков"] = info.get("Знание языков","")
        info["Категории водительских прав"] = info.get("Категории водительских прав", "")
        info["Обязанности"] = info.get("Обязанности", "")
        info["Требования к квалификации"] = info.get("Требования к квалификации", "")

        l = dict(zip(*[iter(raw)]*2))
        item.update(l)
        item.update(info)
        yield item
