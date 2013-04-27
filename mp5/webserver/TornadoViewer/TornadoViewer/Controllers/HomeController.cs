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
        /*
        public ActionResult Index()
        {
            ViewBag.Message = "Modify this template to jump-start your ASP.NET MVC application.";

            return View();
        }
        */
        public ActionResult Index()
        {


            Highcharts chart = new Highcharts("rate")
                .InitChart(new Chart { DefaultSeriesType = ChartTypes.Spline })
                .SetOptions(new GlobalOptions { Global = new Global { UseUTC = false } })
                .SetTitle(new Title { Text = "Packets per destination addresses" })
                .SetSubtitle(new Subtitle { Text = "Storm-aggregated metrics" })
                .SetXAxis(new XAxis
                {
                    Type = AxisTypes.Datetime,
                    TickmarkPlacement = Tickmarks.On,
                })
                .SetYAxis(new YAxis
                {
                    Title = new XAxisTitle { Text = "Packets" },
                    Min = 0
                })
                .SetTooltip(new Tooltip { Formatter = "function() { return '<b>'+ this.series.name +'</b><br/>'+ Highcharts.dateFormat('%F %n %F', this.x) +': '+ this.y +''; }" })
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
            chart.SetSeries(CassandraViewer.Models.MetricRate.GetData(DateTime.MinValue.Ticks, DateTime.MaxValue.Ticks));

            return View(chart);
        }

        public ActionResult Burst()
        {

            Highcharts chart = new Highcharts("burst")
                .InitChart(new Chart { DefaultSeriesType = ChartTypes.Spline })
                .SetOptions(new GlobalOptions { Global = new Global { UseUTC = false } })
                .SetTitle(new Title { Text = "Sources per destination addresses per second" })
                .SetSubtitle(new Subtitle { Text = "Storm-aggregated metrics" })
                .SetXAxis(new XAxis
                {
                    Type = AxisTypes.Datetime,
                    TickmarkPlacement = Tickmarks.On,
                })
                .SetYAxis(new YAxis
                {
                    Title = new XAxisTitle { Text = "Unique IPs" },
                    Min = 0
                })
                .SetTooltip(new Tooltip { Formatter = "function() { return '<b>'+ this.series.name +'</b><br/>'+ Highcharts.dateFormat('%F %n %F', this.x) +': '+ this.y +''; }" })
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
            chart.SetSeries(CassandraViewer.Models.MetricBurst.GetData(DateTime.MinValue.Ticks, DateTime.MaxValue.Ticks));

            return View(chart);
        }

        public ActionResult Abstract()
        {
            return View();
        }
    }
}
