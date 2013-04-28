using System;
using System.Collections.Generic;
using System.Configuration;
using System.Drawing;
using System.Linq;
using System.Web;
using System.Web.Mvc;

using DotNet.Highcharts;
using DotNet.Highcharts.Enums;
using DotNet.Highcharts.Helpers;
using DotNet.Highcharts.Options;
using Point = DotNet.Highcharts.Options.Point;

using CassandraViewer.Models;

namespace CassandraViewer.Controllers
{
    public class HomeController : Controller
    {
        [HttpPost]
        public ActionResult Index(string TimeStart, string TimeEnd)
        {
            Highcharts chart = BuildRateChart();

            DateTime dtStart;
            DateTime dtEnd;

            if (!DateTime.TryParse(TimeStart, out dtStart))
            {
                dtStart = DateTime.MinValue;
            }

            if (!DateTime.TryParse(TimeEnd, out dtEnd))
            {
                dtEnd = DateTime.MaxValue;
            }

            chart.SetSeries(CassandraViewer.Models.MetricRate.GetData(dtStart, dtEnd));

            return View(chart);
         }

        [HttpGet]
        public ActionResult Index()
        {
            Highcharts chart = BuildRateChart();

            chart.SetSeries(CassandraViewer.Models.MetricRate.GetData(DateTime.MinValue, DateTime.MaxValue));
            
            return View(chart);
        }

        public ActionResult Burst()
        {
            Highcharts chart = BuildBurstChart();

            chart.SetSeries(CassandraViewer.Models.MetricBurst.GetData(DateTime.MinValue.Ticks, DateTime.MaxValue.Ticks));

            return View(chart);
        }

        public ActionResult Abstract()
        {
            return View();
        }

        public Highcharts BuildRateChart()
        {
            Highcharts chart = new Highcharts("rate")
                .InitChart(new Chart { 
                    ZoomType = ZoomTypes.X, 
                    DefaultSeriesType = ChartTypes.Spline 
                })
                .SetOptions(new GlobalOptions { Global = new Global { UseUTC = false } })
                .SetTitle(new Title { Text = "Packets per destination addresses" })
                .SetSubtitle(new Subtitle { Text = "Storm-aggregated metrics" })
                .SetXAxis(new XAxis
                {
                    MinRange = 1,
                    Type = AxisTypes.Datetime,
                    TickmarkPlacement = Tickmarks.On,
                })
                .SetYAxis(new YAxis
                {
                    Title = new XAxisTitle { Text = "Packets" },
                    Min = 0
                })
                .SetTooltip(new Tooltip { Formatter = "function() { return '<b>'+ this.series.name +'</b><br>'+ Highcharts.dateFormat('%m/%d/%y', this.x) +'<br>'+ Highcharts.dateFormat('%H:%M:%S', this.x) +'<br><b>'+ this.y +'</b>'; }" })
                .SetPlotOptions(new PlotOptions
                                {
                                    /*
                                    Area = new PlotOptionsArea
                                           {
                                               Stacking = Stackings.Normal,
                                               LineColor = ColorTranslator.FromHtml("#666666"),
                                               LineWidth = 1,
                                               Marker = new PlotOptionsAreaMarker
                                                        {
                                                            LineWidth = 1,
                                                            LineColor = ColorTranslator.FromHtml("#666666")
                                                        }
                                           }*/
                                })
            ;

            return chart;
        }

        public Highcharts BuildBurstChart()
        {
            Highcharts chart = new Highcharts("burst")
                .InitChart(new Chart {
                    ZoomType = ZoomTypes.X,
                    DefaultSeriesType = ChartTypes.Spline
                })
                .SetOptions(new GlobalOptions { Global = new Global { UseUTC = false } })
                .SetTitle(new Title { Text = "Sources per destination addresses" })
                .SetSubtitle(new Subtitle { Text = "Storm-aggregated metrics" })
                .SetXAxis(new XAxis
                {
                    MinRange = 1,
                    Type = AxisTypes.Datetime,
                    TickmarkPlacement = Tickmarks.On,
                })
                .SetYAxis(new YAxis
                {
                    Title = new XAxisTitle { Text = "Unique IPs" },
                    Min = 0
                })
                .SetTooltip(new Tooltip { Formatter = "function() { return '<b>'+ this.series.name +'</b><br>'+ Highcharts.dateFormat('%m/%d/%y', this.x) +'<br>'+ Highcharts.dateFormat('%H:%M:%S', this.x) +'<br><b>'+ this.y +'</b>'; }" })
                .SetPlotOptions(new PlotOptions
                {
                    /*
                    Area = new PlotOptionsArea
                           {
                               Stacking = Stackings.Normal,
                               LineColor = ColorTranslator.FromHtml("#666666"),
                               LineWidth = 1,
                               Marker = new PlotOptionsAreaMarker
                                        {
                                            LineWidth = 1,
                                            LineColor = ColorTranslator.FromHtml("#666666")
                                        }
                           }*/
                })
            ;

            return chart;
        }
    }
}
