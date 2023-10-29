import puppeteer from "puppeteer";
import ExcelJS from "exceljs";
import moment from "moment";
const zone = ['00', '07', '11', '13', '14', '21'];
const timeNow = moment();

// Lấy ngày hiện tại
const day = timeNow.format('DD/MM/YYYY');

// Lấy giờ hiện tại
const hour = timeNow.format('HH:mm:ss');
// Định nghĩa hàm data
const fetchDataForZone = async (zone) => {
  const browser = await puppeteer.launch();
  const page = await browser.newPage();

  const url = `https://www.pnj.com.vn/blog/gia-vang/?zone=${zone}`;
  await page.goto(url);

  // Lấy dữ liệu địa điểm và thời gian
  const diaDiem = await page.evaluate(() => {
    return document.querySelector('#select_gold_area option:checked').textContent;
  });

  const thoiGian = await page.evaluate(() => {
    return document.querySelector('#time-now').textContent;
  });

  // Lấy dữ liệu giá mua và giá bán cho từng địa điểm
  const data = await page.evaluate(() => {
    const rows = Array.from(document.querySelectorAll('#content-price tr'));
    const rowData = rows.map(row => {
      const columns = Array.from(row.querySelectorAll('td'));
      const loaiVang = columns[0].textContent;
      const giaMua = columns[1].querySelector('span').textContent;
      const giaBan = columns[2].querySelector('span').textContent;

      return { loaiVang, giaMua, giaBan };
    });

    return rowData;
  });

  // Đóng trình duyệt
  await browser.close();

  // Thêm cột "Địa điểm" và gán giá trị từ biến zone
  const dataWithZone = data.map(item => ({
    diaDiem: zone,
    ...item,
  }));

  return {ngay: day, diaDiem, thoiGian: hour, dataWithZone };
};

// Sử dụng Promise.all để gọi fetchDataForZone cho tất cả các khu vực
const fetchDataForAllZones = async () => {
  const allData = await Promise.all(zone.map(zone => fetchDataForZone(zone)));

  // Tạo workbook và worksheet
  const workbook = new ExcelJS.Workbook();
  const worksheet = workbook.addWorksheet('GiaVangData');

  // Định dạng cột
  worksheet.columns = [
    { header: 'Ngày', key: 'ngay', width: 20 }, 
    { header: 'Thời gian', key: 'thoiGian', width: 20 }, 
    { header: 'Địa điểm', key: 'diaDiem', width: 20 }, // Thêm cột Địa điểm
    { header: 'Loại vàng', key: 'loaiVang', width: 30 },
    { header: 'Giá mua', key: 'giaMua', width: 15 },
    { header: 'Giá bán', key: 'giaBan', width: 15 },
  ];

  // Ghi dữ liệu từ tất cả các khu vực vào worksheet
  allData.forEach(({ngay,thoiGian, diaDiem, dataWithZone }) => {
    dataWithZone.forEach(item => {
      item.ngay = ngay;
      item.thoiGian = thoiGian;
      item.diaDiem = diaDiem; // Cập nhật cột Địa điểm
      worksheet.addRow(item);
    });
  });

  // Lưu workbook vào một tệp Excel
  const formattedHour = hour.replace(/:/g, '-');
const sanitizedDay = day.replace(/\//g, '-');
const fileName = `${sanitizedDay}_${formattedHour}.xlsx`;

// Sau đó sử dụng fileName trong hàm writeFile
await workbook.xlsx.writeFile(fileName);

  console.log('Đã lưu tất cả dữ liệu vào tệp gia-vang-all-zones.xlsx');
};

// Gọi hàm fetchDataForAllZones để lấy dữ liệu cho tất cả các khu vực và xuất vào cùng một tệp Excel
fetchDataForAllZones();
