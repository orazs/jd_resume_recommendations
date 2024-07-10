import scrapy


class ResumeSpider(scrapy.Spider):
    name = "resume"
    allowed_domains = ["enbek.kz"]
    start_urls = ["https://enbek.kz/ru/search/resume?region_id=75",]

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
        item['url']=response.request.url
        item["name"]= extract_with_css("h4.title strong::text"),
        item["category"] = extract_with_css("div.category::text"),
        item["salary"]= extract_with_css(".price::text"),
        item["bio"]= extract_with_css(".category.mb-2::text"),
        item["area"]= extract_with_css("div.head div div.category.mb-2::text"),
        item['publish_date'] = extract_with_css("ul.info.small.mb-2 li::text")
        info = {}
        flag = response.css("div.label.mb-3::text").extract()
        if "Трудовая деятельность" in flag:
            block = response.css("div.list-date")[0]
            date = block.css(".date span::text").extract()
            values = block.css(".info div.title::text").extract()
            duties = block.css(".info div.description::text").extract()
            values2 = [str(x).strip() for x in values]
            values2 = [x for x in values2 if len(x) > 0]
            values2 = "; ".join(values2)
            info["experience_periods"] = "; ".join(date)
            info["experience_duties"] = "; ".join(duties)
            info["experience"] = values2


        info1 = response.css("ul.info.column.mb-3 li")
        for block in info1:
            labels = block.css("strong::text").extract()
            values = block.css("span *::text").extract()
            values2 = [str(x).strip() for x in values]
            values2 = [x for x in values2 if len(x) > 0]
            values2 = "".join(values2)
            info[labels[0]] = values2




        info_blocks = response.css("div.single-line")
        info_blocks_big = response.css("div.single-line.big")
        info_blocks = info_blocks+info_blocks_big

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
        info["Курсы и сертификаты"] = info.get("Курсы и сертификаты", "")
        info["Опыт работы"] = info.get("Опыт работы","")
        info["experience_periods"] = info.get("experience_periods", "")
        info["experience_duties"] = info.get("experience_duties", "")
        info["experience"] = info.get("experience", "")

        item.update(info)
        yield item
