const puppeteer = require('puppeteer');
const fs = require('fs');

require('dotenv').config();

const FN_USER = process.env.FN_USER;
const FN_PASS = process.env.FN_PASS;


class Fnguide {

  constructor() {
    this.id = FN_USER;
    this.pw = FN_PASS;

    this.width = 1920;
    this.height = 1080;

    this.todayDate = new Date().toISOString().slice(0, 10).replace(/-/gi, '');
  }

  async startBrowser(headlessBool, slowMoTime = 100) {
    const { width, height } = this;

    let puppeteerConfig;

    if (headlessBool) {
      puppeteerConfig = {
        headless: headlessBool,
        args: ['--no-sandbox'],
        slowMo: slowMoTime,
      };
    } else if (!headlessBool) {
      puppeteerConfig = {
        headless: headlessBool,
        args: ['--no-sandbox'],
        slowMo: slowMoTime,
        args: ['--window-size=${width}, ${height}'],
      };
    }
    this.browser = await puppeteer.launch(puppeteerConfig);
    this.page = await this.browser.newPage();

    await this.page.setViewport({ width, height });
    return true;
  }

  async login() {
    const { page } = this;

    const IDInputSelector = '#txtID';
    const PWInputSelector = '#txtPW';
    const loginBtnSelector = '#container > div > div > div.log--wrap > div.log--area > form > div > fieldset > button';
    const logoutOtherIPUserBtnSelector = '#divLogin > div.lay--popFooter > form > button.btn--back';
    const FnguideLogoSelector = 'body > div.header > div > h1 > a';

    await page.goto('https://www.fnguide.com/home/login');
    await page.waitForSelector(IDInputSelector);
    await page.click(IDInputSelector);
    await page.type(IDInputSelector, this.id);
    await page.click(PWInputSelector);
    await page.type(PWInputSelector, this.pw);
    await page.click(loginBtnSelector);

    const logoutOtherIPUserBtnExists = await page.$eval(
      logoutOtherIPUserBtnSelector,
      el => (!!el),
    ).catch((error) => { console.log(error); });

    if (logoutOtherIPUserBtnExists) {
      await page.click(logoutOtherIPUserBtnSelector);
    }

    await page.waitForSelector(FnguideLogoSelector);
  }

  async _requestData(referer, url, returnRaw=false) {
    const { page } = this;

    await page.setExtraHTTPHeaders({
      Referer: referer,
      'X-Requested-With': 'XMLHttpRequest',
    });
    await page.goto(url);
    const rawData = await page.evaluate(() => document.querySelector('body').innerText);

    return (returnRaw) ? JSON.parse(rawData) : JSON.parse(rawData)['Data'][0];
  }

  async getDates(dateFrom='19900101') {
    const { todayDate } = this;

    const data = await this._requestData(
      'http://www.fnguide.com/fgdd/StkIndmByTime',
      `http://www.fnguide.com/api/Fgdd/StkIndMByTimeGrdData?IN_MULTI_VALUE=CJA005930%2CCII.001&IN_START_DT=${dateFrom}&IN_END_DT=${todayDate}&IN_DATE_TYPE=D&IN_ADJ_YN=Y`
    );
    return data.map(d => d.TRD_DT.replace(/\./gi, ''));
  }

  async getTickers(date, market) {
    // (market) kospi: 1, kosdaq: 2
    const data = await this._requestData(
      'http://www.fnguide.com/fgdd/StkIndByTime',
      `http://www.fnguide.com/api/Fgdd/StkIndByTimeGrdDataDate?IN_SEARCH_DT=${date}&IN_SEARCH_TYPE=J&IN_KOS_VALUE=${market}`
    );
    return data.map(d => [d.GICODE, d.ITEMABBRNM]);
  }

  async getETFTickers(date) {
    const data = await this._requestData(
      'http://fnguide.com/fgdd/StkEtf',
      `http://www.fnguide.com/api/Fgdd/StkEtfGrdDataDate?IN_TRD_DT=${date}&IN_MKT_GB=0`
    );
    return data.map(d => [d.GICODE, d.ITEMABBRNM]);
  }

  async getStockInfo() {
    return await this._requestData(
      'http://fnguide.com/fgdd/StkAllItemInfo',
      `http://www.fnguide.com/api/Fgdd/StkAllItemInfoGrdData?IN_KOS_VALUE=0`
    );
  }

   async getCalendarData(date) {
    // date in month format: yyyymm
    return await this._requestData(
      'https://www.fnguide.com/fgdd/calendar',
      `https://www.fnguide.com/data/calendar/${date}.json`,
      true
    );
  }

  async getIndexData(date) {
    return await this._requestData(
      'http://www.fnguide.com/fgdd/StkIndByTime',
      `http://www.fnguide.com/api/Fgdd/StkIndByTimeGrdDataDate?IN_SEARCH_DT=${date}&IN_SEARCH_TYPE=I&IN_KOS_VALUE=0`
    );
  }

  async getFuturesData(date) {
    return await this._requestData(
      'https://www.fnguide.com/fgdd/DrvFtrOptByTime',
      `https://www.fnguide.com/Api/Fgdd/DrvFtrOptByTimeGrdDataDate?IN_CODE=J&IN_DRV_KIND=1&IN_TRD_DT=${date}`
    );
  }

  async getETFData(date) {
    return await this._requestData(
      'http://fnguide.com/fgdd/StkEtf',
      `http://www.fnguide.com/api/Fgdd/StkEtfGrdDataDate?IN_TRD_DT=${date}&IN_MKT_GB=0`
    );
  }

  async getOHLCVData(date) {
    return await this._requestData(
      'http://fnguide.com/fgdd/StkIndByTime',
      `http://www.fnguide.com/api/Fgdd/StkIndByTimeGrdDataDate?IN_SEARCH_DT=${date}&IN_SEARCH_TYPE=J&IN_KOS_VALUE=0`
    );
  }

  async getMktCapData(date) {
    return await this._requestData(
      'http://fnguide.com/fgdd/StkItemDateCap',
      `http://www.fnguide.com/api/Fgdd/StkItemDateCapGrdDataDate?IN_MKT_TYPE=0&IN_SEARCH_DT=${date}`
    );
  }

  async getBuySellData(date) {
    return await this._requestData(
      'http://fnguide.com/fgdd/StkJInvTrdTrend',
      `http://www.fnguide.com/api/Fgdd/StkJInvTrdTrendGrdDataDate?IN_MKT_TYPE=0&IN_TRD_DT=${date}&IN_UNIT_GB=2`
    );
  }

  async getFactorData(date) {
    return await this._requestData(
      'http://www.fnguide.com/fgdd/StkDateShareIndx',
      `http://www.fnguide.com/api/Fgdd/StkDateShareIndxGrdDataDate?IN_SEARCH_DT=${date}&IN_MKT_TYPE=0&IN_CONSOLIDATED=1`
    );
  }

  async getFundamentalData(code, type, period='D', consolidated='1', years='40') {
    /*
    periods      --> IN_GS_GB (연간/분기) : D (연간), Q (분기)
    type         --> IN_ACNT_CODE (보고서종류) : 10 (재무상태표), 20 (포괄손익계산서), 30 (현금흐름표), 40 (자본금변동표), 99 (재무비율)
    consolidated --> IN_CONSOLIDATED (회계기준) : 1 (IFRS연결), 0 (IFRS별도)
    */
    const url = `
    https://www.fnguide.com/api/fgdd/GetFinByIndiv?IN_GICODE=A${code}&IN_GS_GB=${period}&IN_ACCT_STD=I&IN_CONSOLIDATED=${consolidated}&IN_ACNT_CODE=${type}&IN_DETAIL=10&IN_MAXYEAR=${years}
    `;
    return await this._requestData(
      'https://www.fnguide.com/Fgdd/FinIndivCompTrend',
      url,
      true
    );
  }

  async getCompanyEventData(code) {
    return await this._requestData(
      'https://www.fnguide.com/fgdd/StkItemCapInc',
      `https://www.fnguide.com/Api/Fgdd/StkItemCapIncGrdDataTime?IN_CODE_VALUE=A${code}`
    );
  }

  async done() {
    await this.browser.close();
  }
}

function sleep(t) {
  return new Promise(resolve => setTimeout(resolve, t));
}

const getRandomInt = (min, max) => {
  min = Math.ceil(min);
  max = Math.floor(max);
  return Math.floor(Math.random() * (max - min)) + min;
};

const saveToJsonFile = (name, data) => {
  const jsonData = JSON.stringify(data);
  fs.writeFileSync(name, jsonData);
};


(async () => {
  const fn = new Fnguide();
  await fn.startBrowser(false);
  await fn.login();

  const dates = await fn.getDates(dateFrom='20180101');

  for (let date of dates) {
    const path = `G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\futures\\${date}.json`;

    if (!fs.existsSync(path)) {
      const data = await fn.getFuturesData(date);
      saveToJsonFile(path, data);
      const int = getRandomInt(2, 5);
      await sleep(int * 1000);
    }

    console.log(`${date} FUTURES DATA SAVED`);
  }



//  const years = ['2016', '2017', '2018', '2019', '2020', '2021']
//  const months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'];
//  let calendarDates = [];
//  for (let y of years) {
//    for (let m of months) {
//      calendarDates.push(`${y}${m}`);
//    }
//  }
//
//  calendarDates = calendarDates.slice(8);
//
//  for (let date of calendarDates) {
//    const path = `G:\\공유 드라이브\\Project_TBD\\Stock_Data\\fnguide\\calendar\\${date}.json`;
//
//    if (!fs.existsSync(path)) {
//      const data = await fn.getCalendarData(date);
//      console.log(data);
//      saveToJsonFile(path, data);
//      const int = getRandomInt(2, 5);
//      await sleep(int * 1000);
//    }
//
//    console.log(`${date} CALENDAR DATA SAVED`);
//  }



  // const kospi = await fn.getTickers(dates[dates.length - 1], '1');
  // const kosdaq = await fn.getTickers(dates[dates.length - 1], '2');

  // const data = await fn.getETFTickers(dates[dates.length - 2]);
  // console.log(data);

  // const data = await fn.getStockInfo();
  // console.log(data);

  // const index = await fn.getIndexData(dates[dates.length - 1]);
  // console.log(index);

  ///// Fundamental data /////
  // const totalCodes = kospi.concat(kosdaq);
  // const reportPeriods = ['D', 'Q'];
  // const reportTypes = ['10', '20', '30', '40', '99'];
  // const reportConsolidated = ['1'];

  // let cnt = 0;

  // for (let code of totalCodes) {
  //   for (let type of reportTypes) {
  //     for (let period of reportPeriods) {
  //       for (let consolidated of reportConsolidated) {
  //         if (!fs.existsSync(`./data/${code[0]}_${type}_${period}_${consolidated}.json`)) {
  //           let years;
  //           if (period == 'D') {
  //             years = '40';
  //           }
  //           else if (period == 'Q') {
  //             years = '70';
  //           }
  //           if (type == '99') {
  //             years = '20';
  //           }
  //           const data = await fn.getFundamentalData(
  //             code[0], type, period, consolidated, years);
  //           saveToJsonFile(`./data/${code[0]}_${type}_${period}_${consolidated}.json`, data);
  //           const int = getRandomInt(2, 5);
  //           await sleep(int * 1000);
  //         }
  //       }
  //     }
  //   }
  //   cnt++;
  //   console.log(`(${cnt}/${totalCodes.length}) DONE`);
  // }

  await fn.done();
})().catch(e => { console.log(e); });