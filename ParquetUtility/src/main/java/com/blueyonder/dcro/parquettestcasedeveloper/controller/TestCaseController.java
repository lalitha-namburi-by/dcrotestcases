package com.blueyonder.dcro.parquettestcasedeveloper.controller;

import com.blueyonder.dcro.parquettestcasedeveloper.bo.ParquetTestCaseBO;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.DBTestCaseEntity;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.NewTestCaseForm;
import com.blueyonder.dcro.parquettestcasedeveloper.entities.ParquetGrid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.ArrayList;
import java.util.List;

@Controller
public class TestCaseController {

    @Autowired
    private ParquetTestCaseBO parquetTestCaseBO;

    @RequestMapping("/")
    public ModelAndView getHomePage() {
        ModelAndView mav = new ModelAndView("home");
        return mav;
    }

    @RequestMapping("/getDBTestCasePage")
    public ModelAndView getDBTestCasePage() {
        ModelAndView mav = new ModelAndView("dbtestcase");
        return mav;
    }

    @RequestMapping(path = "/getTestCase")
    public ModelAndView getTestCaseData(String testCaseName, String isDirectFilePath) {
        ModelAndView mav = new ModelAndView("index");
        List<ParquetGrid> gridList = null;
        String gridnameslist = null;
        if ("true".equalsIgnoreCase(isDirectFilePath)) {
            gridList = new ArrayList<>();
            gridList.add(parquetTestCaseBO.buildParquetGridUsingFilePath(testCaseName));
            gridnameslist = parquetTestCaseBO.buildGridNamesList(gridList);
        } else {
            gridList = parquetTestCaseBO.buildParquetGridList(testCaseName);
            gridnameslist = parquetTestCaseBO.buildGridNamesList(gridList);
        }
        mav.addObject("isdirectfilepath", isDirectFilePath);
        mav.addObject("gridnameslist", gridnameslist);
        mav.addObject("testcasename", testCaseName);
        mav.addObject("gridlist", gridList);
        return mav;
    }

    @RequestMapping(path = "/getTestCase/{testcase}/gridName/{gridname}")
    public ModelAndView getTestCaseAndParquetData(@PathVariable("testcase") String testCaseName, @PathVariable("gridname") String gridname) {
        ModelAndView mav = new ModelAndView("index");
        List<ParquetGrid> gridList = new ArrayList<>();

        gridList.add(parquetTestCaseBO.buildParquetGrid(testCaseName, gridname));
        String gridnameslist = parquetTestCaseBO.buildGridNamesList(gridList);
        mav.addObject("isdirectfilepath", false);
        mav.addObject("gridnameslist", gridnameslist);
        mav.addObject("testcasename", testCaseName);
        mav.addObject("gridlist", gridList);
        return mav;
    }

    @RequestMapping(path = "/viewParquet")
    public ModelAndView getParquetData(String filepath) {
        ModelAndView mav = new ModelAndView("index");
        List<ParquetGrid> gridList = new ArrayList<>();

        gridList.add(parquetTestCaseBO.buildParquetGridUsingFilePath(filepath));
        String gridnameslist = parquetTestCaseBO.buildGridNamesList(gridList);
        mav.addObject("isdirectfilepath", true);
        mav.addObject("gridnameslist", gridnameslist);
        mav.addObject("testcasename", filepath);
        mav.addObject("gridlist", gridList);
        return mav;
    }

    @RequestMapping(path = "/updateParquet", method = RequestMethod.POST)
    public void updateParquetUsingFilePath(@RequestParam("filePath") String filePath, @RequestBody ParquetGrid grid) {
        parquetTestCaseBO.updateParquetUsingFilePath(filePath, grid);

    }

    @RequestMapping(path = "/save/{testcase}", method = RequestMethod.POST)
    public void updateParquet(@PathVariable("testcase") String testCaseName, @RequestBody ParquetGrid grid) {
        parquetTestCaseBO.writeParquetGrid(testCaseName, grid);

    }

    @RequestMapping(path = "/createTestCaseFromDB", method = RequestMethod.POST)
    public String createTestCaseFromDB(RedirectAttributes redirectAttributes, DBTestCaseEntity grid) {
        parquetTestCaseBO.createTestCaseFromDB(grid);
        redirectAttributes.addAttribute("testCaseName", grid.getTestcasename());
        redirectAttributes.addAttribute("isDirectFilePath", "false");
        return "redirect:/getTestCase";
    }

    @RequestMapping(path = "/createNewBasicTestCase", method = RequestMethod.POST)
    public String createBasicNewTestCase(RedirectAttributes redirectAttributes, NewTestCaseForm newTestCaseForm) {
        parquetTestCaseBO.buildBasicTestCase(newTestCaseForm);
        redirectAttributes.addAttribute("testCaseName", newTestCaseForm.getNewTestCaseName());
        redirectAttributes.addAttribute("isDirectFilePath", "false");
        return "redirect:/getTestCase";
    }

    @RequestMapping(path = "/createNewTestCaseFromAnother", method = RequestMethod.POST)
    public String createNewTestCaseFromAnotherCase(RedirectAttributes redirectAttributes, NewTestCaseForm newTestCaseForm) {
        parquetTestCaseBO.buildNewTestCaseUsingAnotherCase(newTestCaseForm);
        redirectAttributes.addAttribute("testCaseName", newTestCaseForm.getNewTestCaseName());
        redirectAttributes.addAttribute("isDirectFilePath", "false");
        return "redirect:/getTestCase";
    }

    @RequestMapping(value = "/viewBasicTestCaseCreator", method = RequestMethod.GET)
    public ModelAndView getBasicTestCaseCreatorPage() {
        NewTestCaseForm newTestCaseForm = parquetTestCaseBO.buildBasicTestCaseCreatorForm();
        return new ModelAndView("newtestcase", "newTestCaseForm", newTestCaseForm);
    }

    @RequestMapping(value = "/viewTestCaseCreatorFromAnotherTestCase", method = RequestMethod.POST)
    public ModelAndView getTestCaseCreatorFromAnotherTestCasePage(String oldTestCaseName) {
        NewTestCaseForm newTestCaseForm = parquetTestCaseBO.buildTestCaseCreatorFormFromAnotherTestCase(oldTestCaseName);
        return new ModelAndView("oldtonewtestcase", "newTestCaseForm", newTestCaseForm);
    }
    
    @RequestMapping(value = "/createMultipleTestCasesFromDB", method = RequestMethod.GET)
    public String createMultipleTestCasesFromDB() {
        parquetTestCaseBO.createMultipleTestCasesFromDB();
        return "Testcases Created";
    }

}
